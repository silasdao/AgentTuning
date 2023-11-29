from src.task import Task, DataPiece, Dataset
from src.agent import Agent, Session
import pickle
from addict import Dict
from src.tasks.mind2web.dataloader import get_data_split, MultiChoiceDataset, format_input_multichoice
from transformers import AutoTokenizer
from typing import Callable, List, Any, Tuple, Optional, Union
import random
import re
import numpy as np
import json
import time


def fetch_data(session: Session, prompt_template, max_attempts=20):
    for attempt in range(max_attempts):
        try:
            output = session.action(prompt_template)
            assert output is not None
            return output
        except AssertionError:
            print(f"Output is None! Retrying...({attempt + 1}/{max_attempts})")
            time.sleep(3)
    raise RuntimeError("Failed to fetch data after several attempts")

class Mind2Web(Task):
    def __init__(self, **config):
        self.count = config.pop("count", 100000)
        self.range_min = config.pop("range_min", 0)
        self.range_max = config.pop("range_max", 100)
        cfg = Dict(config)
        tokenizer = AutoTokenizer.from_pretrained(cfg.model.model_name_or_path)
        # Load rank of candidates
        candidate_results = None
        if cfg.data.score_file is not None:
            with open(cfg.data.score_file, "rb") as f:
                candidate_results = pickle.load(f)

        self.test_dataset_dict = {}
        for test_key, test_split_file in cfg.data.test_split_files.items():
            test_data = get_data_split(
                cfg.data.data_path,
                test_split_file,
                candidate_results=candidate_results,
                cache_dir=cfg.data.cache_path,
                is_debug=bool(cfg.debug)
            )
            self.test_dataset_dict[test_key] = MultiChoiceDataset(
                test_data,
                tokenizer=tokenizer,
                neg_ratio=cfg.train.neg_ratio,
                num_candidates=cfg.train.num_candidates,
                max_context_len=cfg.train.max_context_len,
            )
        # evaluation configs
        random.seed(cfg.seed)
        self.top_k = cfg.eval.topk
        self.candidates_num = cfg.train.num_candidates
        with open(cfg.llm_prompt, "r") as f:
            self.prompt_template = json.load(f)

        super().__init__(**config)

    @property
    def metrics(self) -> Dict:
        return {
            "score": self.metric,
        }

    def metric(self, output: List[Dict], target: List[Dict]):
        prediction = [x['final_prediction'] for x in output]
        all_element_acc, all_action_f1, all_step_sr = [], [], []
        assert len(target) == len(prediction)
        for pred, tar in zip(prediction, target):
            if tar is None or pred is None:
                all_element_acc.append(0)
                all_action_f1.append(0)                
            else:
                if pred[0] in tar['element']:
                    all_element_acc.append(1)
                else:
                    all_element_acc.append(0)
                all_action_f1.append(
                    self.calculate_f1(pred[1], tar['action'])
                )
            if all_element_acc[-1] > 0 and all_action_f1[-1] >= 1.0:
                all_step_sr.append(1)
            else:
                all_step_sr.append(0)
        return {
            'element_acc': sum(all_element_acc) / len(all_element_acc) * 100,
            'action_f1': np.mean(all_action_f1) * 100,
            'step_sr': np.mean(all_step_sr) * 100,
            'each_f1': all_action_f1,
            'each_sr': all_step_sr,
        }
    
    def get_data(self): 
        ret = Dataset()
        for test_dataset in self.test_dataset_dict.values():
            idx = 0
            for sample in test_dataset.data:
                pos_candidates = sample["pos_candidates"]
                pos_candidates = [c for c in pos_candidates if c["rank"] < self.top_k]
                pos_ids = [c["backend_node_id"] for c in pos_candidates]
                sample.pop("pos_candidates")
                sample["pos_ids"] = pos_ids
                if not pos_ids:
                    ret.append(DataPiece(sample, None))
                    continue
                _, _, target_out, _ = format_input_multichoice(
                    sample, pos_ids[:1], pos_ids[0]
                )
                _, target_action = self.postprocess_action(target_out)
                target = {'element': pos_ids, 'action': target_action}
                ret.append(DataPiece(sample, target))
                idx += 1
                if idx >= self.count:
                    break
        # Candidate generator
        for k in [5, 10, 20, 50]:
            recall_at_k = np.mean(
                [
                    1
                    if any(c["rank"] < k for c in sample["pos_candidates"])
                    else 0
                    for sample in test_dataset.data
                ]
            )
            print(f"Recall Cap @ {k}: {recall_at_k}")
        acc = np.mean(
            [
                1 if any(c["rank"] == 0 for c in sample["pos_candidates"]) else 0
                for sample in test_dataset.data
            ]
        )
        print(f"Candidate generator acc: {acc}")
        return ret          

    def predict_single(self, session: Session, sample: Dict): 
        if len(sample["pos_ids"]) == 0:
            return {"final_prediction":  ('', ''), "outputs": []}
        pos_ids = sample["pos_ids"]
        neg_candidates = sample["neg_candidates"]
        neg_candidates = [c for c in neg_candidates if c["rank"] < self.top_k]
        neg_ids = [c["backend_node_id"] for c in neg_candidates]
        all_candidates = pos_ids + neg_ids
        random.shuffle(all_candidates)
        final_prediction = None
        outputs = []
        while len(all_candidates) > 1:
            candidate_ids = all_candidates[:self.candidates_num] # 5
            all_candidates = all_candidates[self.candidates_num:]
            seq_context, seq_in, _, choices = format_input_multichoice(
                sample, candidate_ids, -1, keep_html_brackets=True
            )
            outputs.append(
                [candidate_ids, [seq_context, seq_in, choices], None]
            )
            self.prompt_template[-1][
                    "content"
                ] = f"'''\n{seq_context}\n'''\n\n{seq_in}"

            session.history = []
            output = fetch_data(session, self.prompt_template)
            # print(session.history[-1])
            # output = "CLICK "
            outputs[-1][-1] = output
            pred_element, pred_action = self.postprocess_action_llm(output)
            if pred_element != "A":
                # convert B, C, D to 0, 1, 2
                pred_element = ord(pred_element) - ord("B")
                try:
                    pred_element = choices[pred_element][0]
                    all_candidates.append(pred_element)
                    final_prediction = (pred_element, pred_action)
                except IndexError:
                    print(f"IndexError: {output}")
        if final_prediction is None or len(all_candidates) == 0:
            final_prediction = ('', '')
        return {"final_prediction": final_prediction, "outputs": outputs}
    
    def postprocess_action(self, text):
        # C.
        # Action: SELECT
        # Value: Queen
        text = text.strip()
        selected_option = text[0]
        action = re.search(r"Action: (CLICK|SELECT|TYPE)", text)
        action = action.group(1) if action is not None else ""
        value = re.search(r"Value: (.*)$", text, re.MULTILINE)
        value = value.group(1) if value is not None else ""
        return selected_option, f"{action.strip()} {value.strip()}"

    def postprocess_action_llm(self, text):
        # C.
        # Action: SELECT
        # Value: Queen
        text = text.strip()
        selected_option = re.search(r"Answer: (A|B|C|D|E|F)", text)
        selected_option = (
            selected_option.group(1) if selected_option is not None else "A"
        )
        action = re.search(r"Action: (CLICK|SELECT|TYPE)", text)
        action = action.group(1) if action is not None else ""
        value = re.search(r"Value: (.*)$", text, re.MULTILINE)
        value = value.group(1) if value is not None else ""
        return selected_option, f"{action.strip()} {value.strip()}"

    def calculate_f1(self, pred, label):
        pred = set(pred.strip().split())
        label = set(label.strip().split())
        if not pred and not label:
            return 1
        if not pred or not label:
            return 0

        tp = len(pred & label)
        fp = len(pred - label)
        fn = len(label - pred)
        precision = tp / (tp + fp)
        recall = tp / (tp + fn)
        if precision == 0 or recall == 0:
            return 0
        return 2 * precision * recall / (precision + recall)
