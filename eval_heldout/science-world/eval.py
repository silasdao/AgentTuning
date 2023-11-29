 
import argparse
import json
import logging
from logging import INFO
import os
import random
import time
import traceback
from typing import Dict, List

from fastchat.conversation import Conversation, SeparatorStyle
from fastchat.model.model_adapter import get_conversation_template
import requests
from requests.exceptions import Timeout
from scienceworld import ScienceWorldEnv
import tiktoken

from eval_utils import findValidActionNew, is_action_failed, load_variation

INIT_PROMPT = '''
Interact with a household to solve a task. Each turn, you can choose from one of the following options:
1. Think: You could think step-by-step to tell your reasoning and planning to solve the task, which will help you handle the task easier.
2. Action: You could interact with the environment freely to solve the task, but remember to refer to your thought and act accordingly.
Prepend your action with "Think: " or "Action: ", e.g. "Think: Now I have picked up the object. Next, I need to move to the location of the answer box." or "Action: go to kitchen".
Exactly only one option could be chosen in a turn.
'''.strip()

CONTROLLER_ADDR = os.environ.get('CONTROLLER_ADDR', '').split(',')

def clean(s):
    clean_toks = ['\n', '\t']
    for tok in clean_toks:
        s = s.replace(tok, ' ')
    return s

def num_tokens_from_messages(messages, model="gpt-3.5-turbo-0613"):
    """Return the number of tokens used by a list of messages."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    if model in {
        "gpt-3.5-turbo-0613",
        "gpt-3.5-turbo-16k-0613",
        "gpt-4-0314",
        "gpt-4-32k-0314",
        "gpt-4-0613",
        "gpt-4-32k-0613",
        }:
        tokens_per_message = 3
        tokens_per_name = 1
    elif model == "gpt-3.5-turbo-0301":
        tokens_per_message = 4  # every message follows <|start|>{role/name}\n{content}<|end|>\n
        tokens_per_name = -1  # if there's a name, the role is omitted
    elif "gpt-3.5-turbo" in model:
        print("Warning: gpt-3.5-turbo may update over time. Returning num tokens assuming gpt-3.5-turbo-0613.")
        return num_tokens_from_messages(messages, model="gpt-3.5-turbo-0613")
    elif "gpt-4" in model:
        print("Warning: gpt-4 may update over time. Returning num tokens assuming gpt-4-0613.")
        return num_tokens_from_messages(messages, model="gpt-4-0613")
    else:
        raise NotImplementedError(
            f"""num_tokens_from_messages() is not implemented for model {model}. See https://github.com/openai/openai-python/blob/main/chatml.md for information on how messages are converted to tokens."""
        )
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
    return num_tokens

def llm_gpt(prompt: List[Dict[str, str]], model: str) -> str:
    if 'OPENAI_API_KEY' not in os.environ:
        raise ValueError("OPENAI_API_KEY must be set to eval GPT models.")

    for _ in range(3):
        try:
            openai_api_key = os.environ['OPENAI_API_KEY']
            openai_api_base = os.environ.get('OPENAI_API_BASE', 'https://api.openai.com/v1')
            response = requests.post(
                f"{openai_api_base}/chat/completions",
                headers={'Authorization': f'Bearer {openai_api_key}'},
                json={
                    'model': model,
                    'messages': prompt,
                    'temperature': 0.0,
                    'max_tokens': 256,
                },
                timeout=120,
            )
            text = response.json()['choices'][0]['message']['content']
            print(text)
            return text.strip()
        except Timeout: 
            print("Timeout, retrying...")
        except ConnectionError:
            print("Connection error, retrying...")
        except Exception:
            traceback.print_exc()
            try:
                print(response)
                print(response.text)
            except:
                pass
        time.sleep(5)
    else:
        raise Exception("Timeout after 3 retries.")

def llm_tgi(prompt: str) -> str:
    data = {
        "inputs": prompt,
        "parameters": {
            "max_new_tokens": 256,
            "do_sample": False,
            'truncate': 4000,
        }
    }
    for _ in range(3):
        try:
            url = f"{random.choice(CONTROLLER_ADDR)}/generate"
            print(f'Sending request to {url} ...')
            response = requests.post(
                url,
                json=data,
                timeout=120,
            )
            text = response.json()["generated_text"]
            print(text)
            return text.split('[INST]')[0].split('<|end_of_turn|>')[0].strip()
        except Timeout: 
            print("Timeout, retrying...")
        except ConnectionError:
            print("Connection error, retrying...")
        except Exception:
            traceback.print_exc()
            try:
                print(response)
                print(response.text)
            except:
                pass
        time.sleep(5)
    else:
        raise Exception("Timeout after 3 retries.")

def get_file_name(args, task_num):
    if (len(args["output_path"]) > 0):
        args["output_path"] = args["output_path"] + "/"

        # Make path if it doesn't exist
        if (not os.path.exists(args['output_path'])):
            try:
                os.makedirs(args["output_path"])
            except:
                pass

    return args["output_path"] + "task" + str(task_num)
  
def process_examples(conv: Conversation, example: List[str]):
    for i, ex in enumerate(example):
        conv.append_message(conv.roles[i % 2], ex)

def get_prompt(conv: Conversation) -> str:
    if conv.name == 'openchat':
        return ''.join(
            f"{role}: {message}{conv.sep}" if message else f"{role}:"
            for role, message in conv.messages
        )
    else:
        return conv.get_prompt()

# Example user input console, to play through a game.
def eval(args, task_num, logger):

    # Initialize environment
    # env = ScienceWorldEnv("", args["jar_path"], envStepLimit = args["env_step_limit"], threadNum = 0)
    env = ScienceWorldEnv("", args["jar_path"], envStepLimit = args["env_step_limit"])
    taskNames = env.getTaskNames()
    taskName = taskNames[task_num]
    env.load(taskName, 0, args['simplification_str'])
    variations = load_variation(env, args, task_num, logger)
    filenameOutPrefixSeed = get_file_name(args, task_num)

    # Load init prompt
    with open(args["prompt_file"], 'r') as f:
        d = json.load(f)

    # Load encoding tool to count token numbers
    token_model = args["model_name"] if 'gpt' in args["model_name"] else 'gpt-4'
    encoding = tiktoken.encoding_for_model(token_model)
    # plans = get_plans(args)

    scores = []

    max_len = 4096

    for variation in variations:

        # train_data = []
        env.load(taskName, variation, args["simplification_str"], generateGoldPath=True)
        task_description = env.taskdescription()[18:]
        recent_actions = ["look around"]

        obs, info = env.reset()

        done = False
        score = 0.0
        last_score = 0.0
        step = 0

        # The env has an internal step count, some actions like look around are free
        # however, the t5 model only generates the action "look around", which will result in a dead loop below
        # so the max_steps here is only used to avoid the model generating the same action forever
        max_steps = args["env_step_limit"] * 2


        if 'gpt' in args["model_name"]:
            conv = get_conversation_template(args["model_name"])
            conv.set_system_message("You are a helpful, respectful and honest assistant.")
        elif 'openchat' in args["model_name"]:
            conv = Conversation(
                name="openchat",
                roles=("GPT4 User", "GPT4 Assistant"),
                messages=[],
                offset=0,
                sep_style=SeparatorStyle.ADD_COLON_SINGLE,
                sep="<|end_of_turn|>",
            )
        elif 'vicuna' in args["model_name"]:
            conv = get_conversation_template('vicuna')
        elif 'llama' in args["model_name"]:
            conv = get_conversation_template('llama-2')
            conv.set_system_message("You are a helpful, respectful and honest assistant.")
        else:
            conv = get_conversation_template(args["model_name"])

        conv.append_message(conv.roles[0], INIT_PROMPT)
        conv.append_message(conv.roles[1], 'Ok.')

        examples = d[str(task_num)]
        process_examples(conv, examples)

        new_task = 'The preceding task has ended. Now, I will start a new task.\n' + clean(obs) + '\n' + task_description
        conv.append_message(conv.roles[0], new_task.strip())

        # Kill agent if it provides more than 10 consecutive invalid actions
        fail_counter = 0

        while not done:
            # Cut the prompt to make it shorter than maximum token numbers
            while len(encoding.encode(get_prompt(conv))) > max_len - 60:
                # Remove the oldest actions in the few-shot
                del conv.messages[4:6]
                # Remove the few-shot if it is empty
                if conv.messages[4][1].startswith('The preceding task has ended.'):
                    del conv.messages[2:4]

            conv.append_message(conv.roles[1], None)

            if 'gpt' in args["model_name"]:
                prompt = conv.to_openai_api_messages()
            else:
                prompt = get_prompt(conv)
            logger.info("###Prompt###\n" + prompt)

            if 'gpt' in args["model_name"]:
                action = llm_gpt(prompt, args["model_name"])
            else:
                action = llm_tgi(prompt)
            logger.info('###Response###\n' + action)

            conv.update_last_message(action)

            # Don't need to actually do think actions
            if action.startswith('Think:'):
                obs = 'OK.'
            else:
                action = action.replace('Action:', '').strip()
                # Get valid actions at this point
                action = findValidActionNew([action], env, info['look'], recent_actions, None, logger)
                obs, _reward, done, info = env.step(action)

                if is_action_failed(obs):
                    fail_counter += 1
                    if fail_counter >= 10:
                        logger.info('Early stop due to consecutive invalid actions')
                        break
                else:
                    fail_counter = 0

                score = info['score']

                if score < 0:
                    # Our own solution for dealing with such cases
                    score = last_score if args["no_stop"] else 0
                    done = True
                last_score = score

            obs = clean(obs)
            print(obs)

            # Add action and observation to game prompt
            conv.append_message(conv.roles[0], obs)

            recent_actions.append(f'({action}, {obs})')

            #logger.info("Input string: " + str(input_str))
            logger.info(f"Variation: {variation}, Step: {step}, Action: {action}")
            logger.info(f"Obs: {obs}")
            logger.info(f"Score: {score}")
            logger.info("")

            step += 1
            if (step >= max_steps) or done:
                break


            logger.info(f"Recent Actions: {recent_actions}")

            # Early stopping if we're in a loop
            if len(recent_actions) >= 5 and len(set(recent_actions[-5:])) == 2:
                logger.info("Many recent actions in history are the same -- model is likely in a loop, stopping early.")
                break


        # Store results
        env.storeRunHistory(variation, notes = {'mode':"react_baseline", 'lm': None} )
        env.saveRunHistoriesBufferIfFull(filenameOutPrefixSeed, maxPerFile=args["max_episode_per_file"])

        scores.append(score)

        logger.info("Run completed...")
        logger.info(f"Scores: {scores}")

        time.sleep(2)

    # Episodes are finished -- manually save any last histories still in the buffer
    env.saveRunHistoriesBufferIfFull(filenameOutPrefixSeed, maxPerFile=args["max_episode_per_file"], forceSave=True)

    avg = sum(scores) / len(scores)
    logger.info(f"Average score: {str(avg)}")

    with open(f"{filenameOutPrefixSeed}-score.txt", "a") as f:
        f.write("\n" + "Task name:" + taskName + "Scores: " + str(scores) + " Average score: " + str(avg) + " Args: " + str(args) + "\n")
    logger.info("Shutting down server...")
    # env.shutdown()

    logger.info("Completed.")



def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jar_path", type=str, default="")
    parser.add_argument("--task_nums", default="0")  # use comma to split 
    parser.add_argument("--env_step_limit", type=int, default=100)
    parser.add_argument("--simplification_str", default="easy")
    parser.add_argument("--max_episode_per_file", type=int, default=9999)
    parser.add_argument("--set", default="test")
    parser.add_argument("--output_path", default="")
    parser.add_argument("--no_stop", action="store_true", default=True)
    parser.add_argument("--prompt_file", default="prompts/prompt.json")
    parser.add_argument("--model_name", default="gpt-4")

    args = parser.parse_args()
    return vars(args)

#
#   Main
#

def init_logger(args, task_num, log_level=INFO):
    filenameOutPrefixSeed = get_file_name(args, task_num)
    logger = logging.getLogger()
    formatter = logging.Formatter("[%(asctime)s][%(levelname)s\t] %(message)s",
                                    datefmt='%Y-%m-%d %H:%M:%S')
    logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    if logging_dir := args["output_path"]:
        os.makedirs(logging_dir, exist_ok=True)
        filename = f"{filenameOutPrefixSeed}.log"
        fh = logging.FileHandler(filename)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.addHandler(fh)
    return logger

def main():
    args = parse_args()
    print(args) 

    task_nums = args["task_nums"].split(",")
    for task_num in task_nums:
        logger = init_logger(args, task_num)
        logger.info(args)
        eval(args, int(task_num), logger)
        
if __name__ == "__main__":
    main()