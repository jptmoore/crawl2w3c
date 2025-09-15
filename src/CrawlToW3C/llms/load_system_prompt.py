import yaml

def load_system_prompt(file_path: str, prompt_name: str):
    with open(file_path, "r", encoding="utf-8") as f:
        prompts = yaml.safe_load(f)
        if prompt_name in prompts:
            return prompts[prompt_name]
        else:
            raise KeyError(f"Prompt name '{prompt_name}' not found.")