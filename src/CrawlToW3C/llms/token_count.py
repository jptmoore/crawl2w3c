import tiktoken
from llama_cpp import Llama

def count_tokens_openai(text: str):
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = len(encoding.encode(text))
    return tokens

def count_tokens_llama_cpp(llm: Llama, text: str):
    return len(llm.tokenize(text.encode("utf-8"), add_bos=False))
