import argparse
import sys
from typing import List, Tuple

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline


def pick_device() -> Tuple[str, int]:
    """
    Returns (device_str, device_index_for_pipeline)
    - device_str: 'cuda' | 'cpu'
    - device_index_for_pipeline: 0 for CUDA, -1 for CPU (as expected by transformers.pipeline)
    """
    if torch.cuda.is_available():
        return ("cuda", 0)
    return ("cpu", -1)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run a local AI assistant with transformers.")
    p.add_argument(
        "--model",
        default="TinyLlama/TinyLlama-1.1B-Chat-v1.0",
        help="Hugging Face model ID or local path",
    )
    p.add_argument("--max-new-tokens", type=int, default=256, help="Max tokens to generate per response.")
    p.add_argument("--temperature", type=float, default=0.7, help="Sampling temperature (higher = more random).")
    p.add_argument("--top-p", type=float, default=0.9, help="Nucleus sampling p.")
    p.add_argument(
        "--system",
        default="You are a helpful assistant.",
        help="System instruction.",
    )
    p.add_argument(
        "--no-history",
        action="store_true",
        help="Do not keep conversation history between turns.",
    )
    return p


def load_model_and_tokenizer(model_id: str):
    # Try without remote code first; fall back to trust_remote_code=True if needed
    try:
        tokenizer = AutoTokenizer.from_pretrained(model_id, use_fast=True)
        model = AutoModelForCausalLM.from_pretrained(model_id, torch_dtype=torch.float32, low_cpu_mem_usage=True)
    except Exception:
        tokenizer = AutoTokenizer.from_pretrained(model_id, use_fast=True, trust_remote_code=True)
        model = AutoModelForCausalLM.from_pretrained(
            model_id, torch_dtype=torch.float32, low_cpu_mem_usage=True, trust_remote_code=True
        )
    return model, tokenizer


def build_prompt(system_prompt: str, history: List[Tuple[str, str]], user: str) -> str:
    # Simple, model-agnostic prompt format suitable for many chat-tuned models
    lines: List[str] = []
    if system_prompt:
        lines.append(f"System: {system_prompt}")
    for u, a in history:
        lines.append(f"User: {u}")
        lines.append(f"Assistant: {a}")
    lines.append(f"User: {user}")
    lines.append("Assistant:")
    return "\n".join(lines)


def main():
    args = build_arg_parser().parse_args()
    device_str, device_index = pick_device()

    print(f"Loading model '{args.model}' on {device_str}...", flush=True)
    model, tokenizer = load_model_and_tokenizer(args.model)

    # Use pipeline for simplicity
    generator = pipeline(
        task="text-generation",
        model=model,
        tokenizer=tokenizer,
        device=device_index,
    )

    print("\nLocal assistant ready. Type your message and press Enter.")
    print("Type 'exit' or 'quit' to leave.\n")

    history: List[Tuple[str, str]] = []
    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if user_input.lower() in {"exit", "quit"}:
            print("Goodbye.")
            break
        if not user_input:
            continue

        prompt = build_prompt(args.system, [] if args.no_history else history, user_input)

        outputs = generator(
            prompt,
            max_new_tokens=args.max_new_tokens,
            do_sample=True,
            temperature=args.temperature,
            top_p=args.top_p,
            pad_token_id=tokenizer.eos_token_id,
            eos_token_id=tokenizer.eos_token_id,
            num_return_sequences=1,
        )

        # Extract only the newly generated part after the last 'Assistant:'
        full_text = outputs[0]["generated_text"]
        assistant_reply = full_text.split("Assistant:")[-1].strip()
        print(f"Assistant: {assistant_reply}\n")

        if not args.no_history:
            history.append((user_input, assistant_reply))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

