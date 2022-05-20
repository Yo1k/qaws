import os
import urllib.request
import json
from typing import Any, Union
from flask import Flask, abort, current_app
from flask import request, g
import psycopg2  # type: ignore
import psycopg2.errors  # type: ignore
import psycopg2.extras # type: ignore

app: Flask = Flask(__name__)


def get_questions(num: int) -> list[dict[str, Any]]:
    with urllib.request.urlopen(url=f"https://jservice.io/api/random?count={num}") as response:
        data: list[dict[str, Any]] = json.loads(response.read())
    return data


def get_db():  # type: ignore
    if "db" not in g:
        g.db = psycopg2.connect(  # pylint: disable=E0237
                dbname="postgres",
                user="postgres",
                password="postgres",
                # host="db",
                host="127.0.0.1",
                port="5432")
        with g.db:
            with g.db.cursor() as cur:
                with current_app.open_resource("questions_schema.sql") as schema:
                    cur.execute(schema.read())
    return g.db

# SKTODO make count for returning
def prepare_updqstn() -> None:
    with g.db:
        with g.db.cursor() as cur:
            cur.execute(
                    """
                    PREPARE updqstn (int[], text[], text[], timestamptz[]) AS
                        INSERT INTO questions
                        (SELECT * from unnest($1, $2, $3, $4))
                        ON CONFLICT (id) DO NOTHING
                        RETURNING count(id);
                    """)


def reformat_information(raw_data):
    key_list = ["id", "question", "answer", "created_at"]
    return [[row[key] for row in raw_data] for key in key_list]


def insert_questions(data) -> int:
    # with g.db:
    with g.db.cursor() as cur:
        cur.execute(
                "EXECUTE updqstn (%s, %s, %s, %s::timestamptz[]);",
                data)
        returning: list[tuple[Any]] = cur.fetchall()
    print(returning)
    print(len(returning))
    return len(returning)


@app.teardown_request
def close_db(_=None) -> None:  # type: ignore
    db = g.pop("db", None)

    if db is not None:
        db.commit()
        db.close()


@app.route('/', methods=['POST'])
def request_questions() -> Union[str, dict[None, None], None]:
    if request.method == "POST":
        questions_num: int = request.get_json().get('questions_num')  # type: ignore
        assert isinstance(questions_num, int) and questions_num >= 0, \
            f"questions_num={questions_num}"
        if questions_num == 0:
            return {}
        else:
            questions_list = get_questions(questions_num)
            prep_data = reformat_information(questions_list)

            get_db()  # type: ignore
            prepare_updqstn()

            retries: int = 100
            while retries > 0:
                retries -= 1
                fail_uniq_num = questions_num - insert_questions(prep_data)
                questions_num = fail_uniq_num

                if fail_uniq_num:
                    questions_list = get_questions(fail_uniq_num)
                    prep_data = reformat_information(questions_list)
                else:
                    return f"{questions_list[-1]['question']}"

            abort(500)
    return None
