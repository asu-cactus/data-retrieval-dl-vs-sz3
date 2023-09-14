import transformers
import os
import time
import torch
import gc

# torch.set_num_threads(1)  # Using single thread
os.environ["TOKENIZERS_PARALLELISM"] = "true"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

from compress import PARTITION_SIZE
from generate_queries import N_QUERIES

BATCH_SIZE = 200
# BATCH_SIZE = min(PARTITION_SIZE, N_QUERIES)
USECOLS = [2, 3, 4, 5, 6]
MAX_LENGTH = 50
CHECKPOINT = "checkpoint-16984000"
# device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
device = torch.device("cpu")


def get_model_and_tokenizer():
    model = transformers.AutoModelForCausalLM.from_pretrained(CHECKPOINT)
    tokenizer = transformers.AutoTokenizer.from_pretrained(CHECKPOINT)
    return (model, tokenizer)


def load_lines(path):
    lines = []
    with open(path, "r") as f:
        for line in f:
            lines.append(line.strip())
    return lines


def make_prompts(indice_file_path):
    lines = load_lines(indice_file_path)
    lines = ["0" * (7 - len(line)) + line + "$" for line in lines]
    return lines


def retrieve(prompts):
    # prompts = ["0000000$", "0000001$", "0000002$", "0000003$"]
    model, tokenizer = get_model_and_tokenizer()
    model.eval()
    model.to(device)

    # Model warmup
    max_new_tokens = MAX_LENGTH
    inputs = tokenizer(["0000000$", "$0000001"], return_tensors="pt").input_ids.to(device)
    model.generate(inputs, max_new_tokens=max_new_tokens, do_sample=False)

    encode_time, inference_time, decode_time, parse_time = 0, 0, 0, 0

    for i in range(0, len(prompts), BATCH_SIZE):
        print(f"The {i}th batch..")
        input_prompts = prompts[i : i + BATCH_SIZE]
        # Start timer
        start_time = time.time()

        # Tokenize inputs
        inputs = tokenizer(input_prompts, return_tensors="pt").input_ids.to(device)
        encode_end_time = time.time()

        # Inference
        outputs = model.generate(inputs, max_new_tokens=max_new_tokens, do_sample=False)
        inference_end_time = time.time()

        del input_prompts, inputs
        gc.collect()

        # Decode
        # predictions = tokenizer.batch_decode(outputs, skip_special_tokens=False)
        predictions = tokenizer.batch_decode(outputs, skip_special_tokens=True)
        # decode_end_time = time.time()

        # Parse results
        results = [
            [
                int(
                    s.split(",")[0]
                    .replace(" ", "")
                    .replace("</s>", "")
                    .replace("[PAD]", "")
                )
                for s in pred.split(":")[1:]
            ]
            for pred in predictions
        ]
        # for pred in predictions:
        #     for s in pred.split(":")[1:]:
        #         int(
        #             s.split(",")[0]
        #             .replace(" ", "")
        #             .replace("</s>", "")
        #             .replace("[PAD]", "")
        #         )
        parse_end_time = time.time()

        del outputs, predictions
        gc.collect()

        # Update times
        encode_time += encode_end_time - start_time
        inference_time += inference_end_time - encode_end_time
        # decode_time += decode_end_time - inference_end_time
        parse_time += parse_end_time - inference_end_time

    # Print results
    print(f"Encode time: {encode_time}s")
    print(f"Inference time: {inference_time}s")
    # print(f"Decode time: {decode_time}s")
    print(f"Parse time: {parse_time}s")
    oveall_time = encode_time + inference_time + decode_time + parse_time
    print(f"Overall time elapsed: {oveall_time}s")


if __name__ == "__main__":
    prompts = make_prompts("outputs/indices/row_level_50_20.txt")
    retrieve(prompts)
