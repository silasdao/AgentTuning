"""
Model implementations. The model interface should be suitable for both
the ``site env'' and the ``text env''.
"""
import json
import random
import re

import requests

random.seed(4)


class BasePolicy:
    def __init__(self):
        pass

    def forward(self, observation, available_actions):
        """
        Args:
            observation (`str`):
                HTML string

            available_actions ():
                ...
        Returns:
            action (`str`): 
                Return string of the format ``action_name[action_arg]''.
                Examples:
                    - search[white shoes]
                    - click[button=Reviews]
                    - click[button=Buy Now]
        """
        raise NotImplementedError


class HumanPolicy(BasePolicy):
    def __init__(self):
        super().__init__()

    def forward(self, observation, available_actions):
        return input('> ')


class RandomPolicy(BasePolicy):
    def __init__(self):
        super().__init__()

    def forward(self, observation, available_actions):
        if available_actions['has_search_bar']:
            return 'search[shoes]'
        action_arg = random.choice(available_actions['clickables'])
        return f'click[{action_arg}]'
