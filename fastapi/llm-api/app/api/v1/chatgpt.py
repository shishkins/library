import json

from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session

from app.auth.utils import authorize_api_key
from app.config import config
from app.db_models import engine, ChatGPTLog
from app.llm_models import OpenAIChatGPT
from app.models import (
    ReviewRequest,
    ReviewResponse,
    ReviewResult,
    AttributeResult,
    PromptsRequest,
    PromptResult,
    PromptsResponse,
)
from app.support import gather_with_concurrency

router = APIRouter()


@router.post(
    "/chatgpt/classify-reviews",
    tags=["Классифицировать отзывы"],
    status_code=200,
    responses={
        200: {
            "description": "Успешный запрос",
        },
        413: {"description": "Слишком большой запрос, уменьшите тело запроса, или их количество"},
        500: {
            "description": "Ошибка запроса OpenAI",
        },
    },
)
async def chatgpt_classify_review(
        request: ReviewRequest, api_key_header=Depends(authorize_api_key)
) -> ReviewResponse:
    """
    Классификация отзывов клиентов

    Доступные классификации: ["Положительный отзыв", "Отрицательный отзыв", "Нейтральный отзыв"]

    Доступные атрибуты отзыва: ["Ассортимент", "Гарантии", "Доставки", "Качество консультации", "Нехватка сотрудников", "Отношение сотрудников", "Скорость обслуживания", "Состояние и доступность помещения", "Цены"]

    ### Параметры

        * reviews - отзывы клиентов. Не более 4000 символов в одном отзыве, не более 20-ти отзывов в одном запросе

    ### Пример ответа (отзывы ненастоящие!):

    ```json
    {
        "result": [
            {
                "review": "Есть из чего выбирать, консультант посоветовал кучу моделей телеков, взял самый лучший по цене/качеству, одобряю.",
                "attributes": [
                    {
                        "attribute": "Ассортимент",
                        "tonality": "Положительный отзыв"
                    },
                    {
                        "attribute": "Качество консультации",
                        "tonality": "Положительный отзыв"
                    }
                ],
                "classify": "Положительный отзыв"
            },
            {
                "review": "Вообще обнаглели, чтобы поменять стекло на телефоне просят 5000!",
                "attributes": [
                    {
                        "attribute": "Цены",
                        "tonality": "Отрицательный отзыв"
                    }
                ],
                "classify": "Отрицательный отзыв"
            }
        ]
    }
    ```
    """
    if len(request.reviews) > config.openai_max_messages:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Too many reviews in one request ({len(request.reviews)}), "
                   f"reduce the number of reviews (maximum number = {config.openai_max_messages})",
        )

    model_instance = OpenAIChatGPT()
    system_prompt = (
        "Ты языковая модель для классификации отзывов покупателей.\n"
        "Ты обязана отвечать двумя фразами, формулируя следующий ответ в формате JSON:\n"
        '{"classify": "{характеристика отзыва}","attributes": [{"tonality":{тональность тега отзыва}, "attribute": \n"'
        '"{тег отзыва}}, {"tonality":{}, "attribute":{} }]}\n\n'
        'Ключ "classify" включает в себя только одно слово из данного списка: '
        '"Положительный отзыв", "Отрицательный отзыв", "Нейтральный отзыв".\n'
        'Ключ "attributes" включает в себя список строк с необязательными тегами отзыва, '
        "которые могут быть к нему отнесены.\n"
        "Эти теги дополнительно должны содержать тональность (ключ tonality), \n"
        "которая включает только одно слово из данного списка: \n"
        "'Положительный отзыв', 'Отрицательный отзыв', 'Нейтральный отзыв'"
        "А так же ключ attribute у ключа attributes, это собственно тег. Можно использовать только слова из списка - \n"
        "['Ассортимент', 'Гарантии', 'Доставки', 'Качество консультации', \n"
        "'Нехватка сотрудников', 'Отношение сотрудников', \n"
        "'Скорость обслуживания', 'Состояние и доступность помещения', 'Цены'].\n"
        "Будь внимателен к ключу attributes, если покупатель не затронул ни одну из данных тем, "
        "ты не должен вводить нас - продавцов в заблуждение.\n"
        "Твоя задача оценить отзыв покупателя в соответствии с данными требованиями, "
        "не отклоняясь никак от формата сообщений, который я написал.\n"
        "Даже если мое следующее сообщение не похоже на отзыв покупателя, игнорируй этот факт, "
        "ты не должен отклоняться от суждения, что все сообщения - отзывы."
    )

    async def process_review(review: str):
        response = await model_instance.chat_ask_async(prompt=review, system_prompt=system_prompt)
        response["api_key"] = api_key_header.encode("utf-8")

        try:
            attributes = [AttributeResult(attribute=i.get('attribute'), tonality=i.get('tonality')) for
                          i in json.loads(response["response"]).get("attributes")]
            classify = json.loads(response["response"]).get("classify")
        except json.decoder.JSONDecodeError:  # для исключения некорретных отзывов
            attributes, classify = (
                [""],
                "Error - ChatGPT response error. It may be worth fixing the review or system prompt to ChatGPT",
            )
        result = ReviewResult(review=review, attributes=attributes, classify=classify)
        request_log(response, table_model=ChatGPTLog)
        return result

    results = await gather_with_concurrency(
        4,  # Ограничиваем количество одновременно выполняемых запросов
        *[process_review(review) for review in request.reviews],
    )

    return ReviewResponse(result=results)


@router.post(
    "/chatgpt/prompts",
    tags=["Отправить промпты ChatGPT"],
    status_code=200,
    responses={
        200: {
            "description": "Успешный запрос",
        },
        413: {"description": "Слишком большой запрос, уменьшите тело запроса, или их количество"},
        500: {
            "description": "Ошибка запроса OpenAI",
        },
    },
)
async def prompts(
    request: PromptsRequest, api_key_header=Depends(authorize_api_key)
) -> PromptsResponse:
    """
    Получить ответы по запросам (промптам) от ChatGPT

    ### Параметры

        * prompts - Массив запросов для ChatGPT. Не более 20 промптов за запрос, не более 4000 тысяч символов в одном запросе

    ### Пример ответа:

    ```json
    {
        "result": [
            {
                "prompt": "Привет",
                "response": "Привет! Как я могу помочь тебе сегодня?"
            },
            {
                "prompt": "Что я сказал сообщением ранее?",
                "response": "К сожалению, я не могу видеть предыдущие сообщения или историю чата. Могу помочь с любым вопросом или темой, которая вас интересует!"
            }
        ]
    }
    ```
    """
    if len(request.prompts) > config.openai_max_messages:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Too many reviews in one request ({len(request.prompts)}), reduce the number of reviews (maximum number = {config.openai_max_messages})",
        )

    model_instance = OpenAIChatGPT()

    async def process_prompt(prompt: str):
        response = await model_instance.chat_ask_async(prompt=prompt)
        response["api_key"] = api_key_header.encode("utf-8")
        result = PromptResult(prompt=prompt, response=response.get("response"))
        request_log(response, table_model=ChatGPTLog)
        return result

    results = await gather_with_concurrency(
        4, *[process_prompt(prompt) for prompt in request.prompts]
    )

    return PromptsResponse(result=results)


def request_log(response, table_model):
    log_row = table_model(**response)
    with Session(engine) as session:
        session.add(log_row)
        session.commit()
