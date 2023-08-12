import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Обращается к API Ozon Seller за списком созданных товаров.

    Args:
        last_id (str): Идентификатор последнего значения на странице.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): API-ключ Ozon.

    Returns:
        dict: Ответ Ozon со списком товаров, их количеством и
        идентификатором последнего значения на странице.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> get_product_list("", client_id, seller_token)
        {
            "result": {
                "items": [
                    {
                        "product_id": 223681945,
                        "offer_id": "136748"
                    }
                ],
                "total": 1,
                "last_id": "bnVсbA=="
            }
        }

        >>> get_product_list("", client_id, seller_token)
        {
            "code": 0,
            "details": [
                {
                    "typeUrl": "string",
                    "value": "string"
                }
            ],
            "message": "string"
        }
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Постранично запрашивает артикулы созданных товаров у API Ozon Seller.

    Args:
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): API-ключ Ozon.

    Returns:
        offer_ids (list): Список артикулов товаров озон.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> get_offer_ids(client_id, seller_token)
        ["136748", "168448", "528148", "236297"]

        >>> get_offer_ids(client_id, seller_token)
        []
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

    Обновляет на Ozon информацию о стоимости остатков до 1000 товаров.

    Args:
        prices (list): Список новых цен товаров.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): API-ключ Ozon.

    Returns:
        dict: Ответ API Ozon в формате json об успешности операции.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> update_price(prices, client_id, seller_token)
        {
            "result": [
                {
                    "product_id": 1386,
                    "offer_id": "PH8865",
                    "updated": true,
                    "errors": []
                }
            ]
        }

        >>> update_price(prices, client_id, seller_token)
        {
            "code": 0,
            "details": [
                {
                    "typeUrl": "string",
                    "value": "string"
                }
            ],
            "message": "string"
        }
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

    Обновляет на Ozon информацию о количестве товаров.

    Args:
        stocks (list): Список обновленного количества товаров.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): API-ключ Ozon.

    Returns:
        dict: Ответ API Ozon в формате json об успешности операции.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> update_stocks(stocks, client_id, seller_token)
        {
            "result": [
                {
                    "product_id": 55946,
                    "offer_id": "PG-2404С1",
                    "updated": true,
                    "errors": []
                }
            ]
        }

        >>> update_stocks(stocks, client_id, seller_token)
        {
            "code": 0,
            "details": [
                {
                    "typeUrl": "string",
                    "value": "string"
                }
            ],
            "message": "string"
        }
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.

    Скачивает архив с файлом остатков товаров с сайта timeworld.ru,
    преобразуя записи из него в словари.

    Returns:
        watch_remnants (list): Список словарей с информацией о товарах.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> download_stock()
        [
            {
                "Код": "48852",
                "Наименование товара": "B 4204 LSSF",
                "Изображение": "http://www.timeworld.ru/products/itshow.php?id=48857",
                "Цена": "24'570.00 руб.",
                "Количество": "1",
                "Заказ": ""
            }
        ]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Сформировать список товаров в наличии и их количества.

    На основании данных с сайта timeworld.ru заполняет информацию об
    остатках для Ozon.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        offer_ids (list): Список артикулов товаров озон.

    Returns:
        stocks (list): Список обновленного количества товаров.

    Examples:
        >>> create_stocks(watch_remnants, offer_ids)
        [
            {
                "offer_id": "48852",
                "stock": 0
            },
            {
                "offer_id": "48857",
                "stock": 2
            }
        ]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Обновление цен товаров.

    На основании данных с сайта timeworld.ru заполняет информацию о новых
    ценах для Ozon.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        offer_ids (list): Список артикулов товаров озон.

    Returns:
        prices (list): Список новых цен товаров.

    Examples:
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "48852",
                "old_price": "0",
                "price": "24570",
            }
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену к единому формату.

    Удаляет все символы, кроме цифр, и убирает копейки.

    Args:
        price (str): Цена в произвольном формате.

    Returns:
        str: Унифицированная цена.

    Examples:
        >>> price_conversion("24'570.00 руб.")
        "24570"
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Args:
        lst (list): Список разделяемых элементов.
        n (int): Максимальное количество элементов в части.

    Returns:
        list: Подсписок оригинального списка, содержащий не более n элементов.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5, 6, 7, 8, 9], 3))
        [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
        ]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить список цен на Ozon.

    Обновляет цены товаров на Ozon в соответствии с полученными в
    watch_remnants данными.

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): API-ключ Ozon.

    Returns:
        prices (list): Список новых цен товаров.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> upload_prices(watch_remnants, client_id, seller_token)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "48852",
                "old_price": "0",
                "price": "24570",
            }
        ]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить количество товаров на Ozon.

    Обновляет количество товаров на Ozon в соответствии с полученными в
    watch_remnants данными. Формирует отдельным списком товары, которые
    есть в наличии. 

    Args:
        watch_remnants (list): Список словарей с информацией о товарах.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): API-ключ Ozon.

    Returns:
        not_empty (list): Список товаров, которые есть в наличии.
        stocks (list): Список обновленного количества товаров.

    Raises:
        HTTPError: Если код ответа не 200.

    Examples:
        >>> upload_stocks(watch_remnants, client_id, seller_token)
        [
            {
                "offer_id": "48857",
                "stock": 2
            }
        ],
        [
            {
                "offer_id": "48852",
                "stock": 0
            },
            {
                "offer_id": "48857",
                "stock": 2
            }
        ]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
