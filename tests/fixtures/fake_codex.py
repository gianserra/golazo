#!/usr/bin/env python3
"""Small Codex JSONL stand-in used for browser smoke tests."""

import json
import sys
import time


thread_id = next((argument for argument in sys.argv if argument.startswith("thread-")), "thread-ui-demo")
print(json.dumps({"type": "thread.started", "thread_id": thread_id}), flush=True)
print(json.dumps({"type": "turn.started"}), flush=True)
print(json.dumps({"type": "item.started", "item": {"type": "command_execution", "command": "pytest"}}), flush=True)
time.sleep(0.15)
print(
    json.dumps(
        {
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": "I added the next implementation slice, verified the API contract, and updated the goal tracker.",
            },
        }
    ),
    flush=True,
)
print(
    json.dumps(
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 12480,
                "cached_input_tokens": 9100,
                "output_tokens": 842,
                "reasoning_output_tokens": 311,
            },
        }
    ),
    flush=True,
)
