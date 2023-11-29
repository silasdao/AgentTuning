import argparse
import json
import os
import random
import time
import traceback
from typing import Any

import tiktoken
from beartype import beartype
from beartype.door import is_bearable

from agent.prompts import *
from browser_env import Trajectory
from browser_env.actions import (
    Action,
    ActionParsingError,
    create_id_based_action,
    create_none_action,
    create_playwright_action,
)
from browser_env.utils import Observation, StateInfo
from llms import lm_config
from llms.providers.openai_utils import (
    generate_from_openai_chat_completion,
    generate_from_openai_completion,
)

import requests
from requests.exceptions import Timeout

import urllib3

urllib3.disable_warnings()

CONTROLLER_ADDR = os.environ['CONTROLLER_ADDR'].split(',')

def llm_llama(prompt: str, config: lm_config.LMConfig) -> str:
    data = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": config.gen_config['max_tokens'],
            "do_sample": True,
            'temperature': config.gen_config['temperature'],
            'top_p': config.gen_config['top_p'],
            'truncate': 4000,
        }
    }
    for _ in range(5):
        try:
            response = requests.post(
                f"{random.choice(CONTROLLER_ADDR)}/generate",
                json=data,
                timeout=120,
                proxies={'http': '', 'https': ''},
            )
            return response.json()["generated_text"]
        except Timeout: 
            print("Timeout, retrying...")
        except ConnectionError:
            print("Connection error, retrying...")
        except Exception as e:
            traceback.print_exc()
            try:
                print(response)
                print(response.text)
            except:
                pass
        time.sleep(5)
    else:
        raise Exception("Timeout after 5 retries.")

class Agent:
    """Base class for the agent"""

    def __init__(self, *args: Any) -> None:
        pass

    def next_action(
        self, trajectory: Trajectory, intent: str, meta_data: Any
    ) -> Action:
        """Predict the next action given the observation"""
        raise NotImplementedError

    def reset(
        self,
        test_config_file: str,
    ) -> None:
        raise NotImplementedError


class TeacherForcingAgent(Agent):
    """Agent that follows a pre-defined action sequence"""

    def __init__(self) -> None:
        super().__init__()

    @beartype
    def set_action_set_tag(self, tag: str) -> None:
        self.action_set_tag = tag

    @beartype
    def set_actions(self, action_seq: str | list[str]) -> None:
        if isinstance(action_seq, str):
            action_strs = action_seq.strip().split("\n")
        else:
            action_strs = action_seq
        action_strs = [a.strip() for a in action_strs]

        actions = []
        for a_str in action_strs:
            try:
                if self.action_set_tag == "playwright":
                    cur_action = create_playwright_action(a_str)
                elif self.action_set_tag == "id_accessibility_tree":
                    cur_action = create_id_based_action(a_str)
                else:
                    raise ValueError(
                        f"Unknown action type {self.action_set_tag}"
                    )
            except ActionParsingError as e:
                cur_action = create_none_action()

            cur_action["raw_prediction"] = a_str
            actions.append(cur_action)

        self.actions: list[Action] = actions

    @beartype
    def next_action(
        self, trajectory: Trajectory, intent: str, meta_data: Any
    ) -> Action:
        """Predict the next action given the observation"""
        return self.actions.pop(0)

    @beartype
    def reset(
        self,
        test_config_file: str,
    ) -> None:
        with open(test_config_file) as f:
            ref_actions = json.load(f)["reference_action_sequence"]
            tag = ref_actions["action_set_tag"]
            action_seq = ref_actions["action_sequence"]
            self.set_action_set_tag(tag)
            self.set_actions(action_seq)


class PromptAgent(Agent):
    """prompt-based agent that emits action given the history"""

    def __init__(
        self,
        action_set_tag: str,
        lm_config: lm_config.LMConfig,
        prompt_constructor: PromptConstructor,
    ) -> None:
        super().__init__()
        self.lm_config = lm_config
        self.prompt_constructor = prompt_constructor
        self.action_set_tag = action_set_tag

    @beartype
    def set_action_set_tag(self, tag: str) -> None:
        self.action_set_tag = tag

    # @beartype
    def next_action(
        self, trajectory: Trajectory, intent: str, meta_data: dict[str, Any]
    ) -> (Action, str, str):
        prompt = self.prompt_constructor.construct(
            trajectory, intent, meta_data
        )
        lm_config = self.lm_config
        if lm_config.provider == "openai":
            if lm_config.mode == "chat":
                response = generate_from_openai_chat_completion(
                    messages=prompt,
                    model=lm_config.model,
                    temperature=lm_config.gen_config["temperature"],
                    top_p=lm_config.gen_config["top_p"],
                    context_length=lm_config.gen_config["context_length"],
                    max_tokens=lm_config.gen_config["max_tokens"],
                    stop_token=None,
                )
            elif lm_config.mode == "completion":
                response = generate_from_openai_completion(
                    prompt=prompt,
                    engine=lm_config.model,
                    temperature=lm_config.gen_config["temperature"],
                    max_tokens=lm_config.gen_config["max_tokens"],
                    top_p=lm_config.gen_config["top_p"],
                    stop_token=lm_config.gen_config["stop_token"],
                )
            else:
                raise ValueError(
                    f"OpenAI models do not support mode {lm_config.mode}"
                )
        elif lm_config.provider == 'llama':
            response = llm_llama(prompt, lm_config)
        else:
            raise NotImplementedError(
                f"Provider {lm_config.provider} not implemented"
            )

        print(response)

        try:
            parsed_response = self.prompt_constructor.extract_action(response)
            if self.action_set_tag == "id_accessibility_tree":
                action = create_id_based_action(parsed_response)
            elif self.action_set_tag == "playwright":
                action = create_playwright_action(parsed_response)
            else:
                raise ValueError(f"Unknown action type {self.action_set_tag}")

            action["raw_prediction"] = response

        except ActionParsingError as e:
            action = create_none_action()
            action["raw_prediction"] = response

        return action, prompt, response

    def reset(self, test_config_file: str) -> None:
        pass


def construct_llm_config(args: argparse.Namespace) -> lm_config.LMConfig:
    llm_config = lm_config.LMConfig(
        provider=args.provider, model=args.model, mode=args.mode
    )
    if args.provider not in {'openai', 'llama'}:
        raise NotImplementedError(f"provider {args.provider} not implemented")
    llm_config.gen_config["temperature"] = args.temperature
    llm_config.gen_config["top_p"] = args.top_p
    llm_config.gen_config["context_length"] = args.context_length
    llm_config.gen_config["max_tokens"] = args.max_tokens
    llm_config.gen_config["stop_token"] = args.stop_token
    llm_config.gen_config["max_obs_length"] = args.max_obs_length
    return llm_config


def construct_agent(args: argparse.Namespace) -> Agent:
    llm_config = construct_llm_config(args)

    agent: Agent
    if args.agent_type == "teacher_forcing":
        agent = TeacherForcingAgent()
    elif args.agent_type == "prompt":
        with open(args.instruction_path) as f:
            constructor_type = json.load(f)["meta_data"]["prompt_constructor"]
        if llm_config.provider in {'llama'}:
            tokenizer = tiktoken.encoding_for_model('gpt-4')
        else: tokenizer = tiktoken.encoding_for_model(llm_config.model)
        prompt_constructor = eval(constructor_type)(
            args.instruction_path, lm_config=llm_config, tokenizer=tokenizer
        )
        agent = PromptAgent(
            action_set_tag=args.action_set_tag,
            lm_config=llm_config,
            prompt_constructor=prompt_constructor,
        )
    else:
        raise NotImplementedError(
            f"agent type {args.agent_type} not implemented"
        )
    return agent
