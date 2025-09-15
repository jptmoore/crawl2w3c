from llama_cpp import Llama, LlamaGrammar


grammar = LlamaGrammar.from_string(r'''
        root ::= choice
        choice ::= "archive" | "skip"''')


def get_client(model_path: str = "models/tinyllama-1.1b-chat-v1.0.Q5_K_M.gguf"):
    return Llama(model_path=model_path, n_ctx=2048, verbose=False)

def generate_response(llm, system_prompt: str, user_prompt: str, annotation: bool = True):
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    params = {
        "messages": messages,
        "temperature": 0,
    }
    if not annotation:
        params["grammar"] = grammar

    try:
        response = llm.create_chat_completion(**params)
    except ValueError:
        return None

    return response["choices"][0]["message"]["content"].strip()



if __name__ == "__main__":
    anno = generate_response(
        system_prompt="You are a helpful assistant. Annotate this text:",
        user_prompt="The quick brown fox jumps over the lazy dog.")
    
    print(anno)