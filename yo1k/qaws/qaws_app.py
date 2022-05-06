import os
import urllib.request
import json
from typing import Any, Union
from flask import Flask, abort, current_app
from flask import request, g
import psycopg2  # type: ignore
import psycopg2.errors  # type: ignore

app: Flask = Flask(__name__)


def get_questions(num: int) -> list[dict[str, Any]]:
    with urllib.request.urlopen(url=f"https://jservice.io/api/random?count={num}") as response:
        data: list[dict[str, Any]] = json.loads(response.read())
    return data


def get_db():  # type: ignore
    if "db" not in g:
        g.db = psycopg2.connect(  # pylint: disable=E0237
                dbname=os.environ.get("POSTGRES_NAME"),
                user=os.environ.get("POSTGRES_USER"),
                password=os.environ.get("POSTGRES_PASSWORD"),
                host="db",
                port="5432")
        with g.db:
            with g.db.cursor() as cur:
                with current_app.open_resource("questions_schema.sql") as schema:
                    cur.execute(schema.read())
    return g.db


def prepare_updqstn() -> None:
    with g.db:
        with g.db.cursor() as cur:
            cur.execute(
                    """
                    PREPARE updqstn (integer, text, text, timestamptz) AS
                        INSERT INTO questions VALUES($1, $2, $3, $4);
                    """)


def insert_question(id_q: int, question: str, answer: str, created_at: str) -> bool:
    try:
        with g.db:
            with g.db.cursor() as cur:
                cur.execute(
                        "EXECUTE updqstn(%(id)s, %(question)s, %(answer)s, %(created_at)s);",
                        {
                                "id": id_q,
                                "question": question,
                                "answer": answer,
                                "created_at": created_at})
                return False
    except psycopg2.errors.lookup("23505"):
        return True


@app.teardown_request
def close_db(_=None) -> None:  # type: ignore
    db = g.pop("db", None)

    if db is not None:
        db.close()


@app.route('/', methods=['POST'])
def questions_request() -> Union[str, dict[None, None], None]:
    if request.method == "POST":
        questions_num: int = request.get_json().get('questions_num')  # type: ignore
        assert isinstance(questions_num, int) and questions_num >= 0, \
            f"questions_num={questions_num}"
        if questions_num == 0:
            return {}
        else:
            questions_list = get_questions(questions_num)
            get_db()  # type: ignore
            prepare_updqstn()

            retries: int = 1000
            while retries > 0:
                retries -= 1
                fail_uniq_num: int = 0

                for qstn in questions_list:
                    fail_uniq = insert_question(
                            qstn['id'],
                            qstn['question'],
                            qstn['answer'],
                            qstn['created_at'])
                    if fail_uniq:
                        fail_uniq_num += 1

                if fail_uniq_num:
                    questions_list = get_questions(fail_uniq_num)
                else:
                    return f"{questions_list[-1]['question']}"

            abort(500)
    return None
