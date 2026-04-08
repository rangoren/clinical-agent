from functools import lru_cache

from settings import (
    R2_ACCESS_KEY_ID,
    R2_ACCOUNT_ID,
    R2_BUCKET_NAME,
    R2_ENDPOINT,
    R2_SECRET_ACCESS_KEY,
)


BOOK_OBJECTS = [
    {
        "key": "gabbe_obstetrics_9.pdf",
        "book_id": "gabbe_9",
        "title": "Gabbe's Obstetrics: Normal and Problem Pregnancies",
        "edition": "9",
        "domain": "obstetrics",
    },
    {
        "key": "berek_novak_gynecology_17.pdf",
        "book_id": "berek_17",
        "title": "Berek & Novak's Gynecology",
        "edition": "17",
        "domain": "gynecology",
    },
    {
        "key": "speroff_rei_10.pdf",
        "book_id": "speroff_10",
        "title": "Speroff's Clinical Gynecologic Endocrinology and Infertility",
        "edition": "10",
        "domain": "fertility",
    },
]


def is_r2_configured():
    return all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_ENDPOINT])


def get_book_objects():
    return list(BOOK_OBJECTS)


@lru_cache(maxsize=1)
def get_r2_client():
    if not is_r2_configured():
        raise RuntimeError("R2 is not fully configured.")

    import boto3
    from botocore.client import Config

    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def build_r2_object_url(object_key):
    return f"{R2_ENDPOINT}/{R2_BUCKET_NAME}/{object_key}"
