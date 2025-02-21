import asyncio

import httpx
import tiktoken
from fastapi import HTTPException, status
from openai import OpenAI
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.config import config


class OpenAIChatGPT:
    def __init__(self):
        class Settings(BaseSettings):
            openai_api_key: str
            openai_proxy_url: str
            openai_chat_model: str = "gpt-4o-mini"

            model_config = SettingsConfigDict(case_sensitive=False)

        self.config = Settings()

    def get_openai_client(self):
        return OpenAI(
            api_key=self.config.openai_api_key,
            http_client=httpx.Client(proxy=self.config.openai_proxy_url),
        )

    async def chat_ask_async(self, prompt: str, system_prompt: str = "Ты полезный помощник."):
        client = self.get_openai_client()
        messages = [
            {
                "role": "developer",
                "content": [{"type": "text", "text": system_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            },
        ]

        if len(prompt) > config.openai_max_message_length:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Too many characters in prompt - {len(prompt)}, "
                f"reduce the number of characters (maximum len = {config.openai_max_message_length})",
            )
        else:
            chat = await asyncio.to_thread(
                client.chat.completions.create,
                model=self.config.openai_chat_model,
                messages=messages,
                stream=False,
                timeout=10,
            )
            return {
                "system_prompt": system_prompt,
                "prompt": prompt,
                "response": chat.choices[0].message.content,
                "model": self.config.openai_chat_model,
                "input_tokens": self.count_tokens(prompt, self.config.openai_chat_model)
                + self.count_tokens(system_prompt, self.config.openai_chat_model),
                "output_tokens": self.count_tokens(
                    chat.choices[0].message.content, self.config.openai_chat_model
                ),
            }

    @staticmethod
    def count_tokens(content: str, openai_chat_model: str) -> int:
        encoding = tiktoken.encoding_for_model(openai_chat_model)
        return len(encoding.encode(content))
