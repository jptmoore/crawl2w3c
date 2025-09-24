import tiktoken

def count_tokens_openai(text: str):
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = len(encoding.encode(text))
    return tokens
