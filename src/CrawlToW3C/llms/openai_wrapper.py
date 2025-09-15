import os
from openai import OpenAI

def get_client():
    """Must have .env variable 'OPENAI_API_KEY' set"""
    return OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_response(llm, system_prompt:str, user_prompt:str, model: str="gpt-5"):
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

    response = llm.chat.completions.create(
        model=model,
        messages=messages,
        temperature=1,
        reasoning_effort="minimal",
        response_format={"type": "json_object"}
    )

    content = response.choices[0].message.content.strip()
    return content

