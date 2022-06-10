import urllib.request
import json
from abc import ABC
from typing import Any, Union, Optional, Generic, TypeVar, Sequence
from flask import Flask, abort, current_app
from flask import request, g
import psycopg
import psycopg.errors


T = TypeVar("T")
JSONType = dict[str, Any]


class QuestionService(ABC, Generic[T]):
    def get_questions(self, num: int) -> Sequence[T]:
        pass


class StorageService(ABC):
    def insert_uniq_questions(self, data: list[list[Any]]):
        pass


class JSONQuestionService(QuestionService[JSONType]):
    def __init__(self, url="https://jservice.io/api/random?count="):
        self.__url = url

    def get_questions(self, num: int) -> Sequence[JSONType]:
        with urllib.request.urlopen(url=f"{self.__url}{num}") as response:
            data: Sequence[JSONType] = json.loads(response.read())
        return data


class DefaultQuestionService(QuestionService[Sequence[Any]]): #SKTODO ABC
    def __init__(self, delegate: JSONQuestionService):
        self.__delegate = delegate
        self.__cache_json_data = []  # SKTODO choose the right data structure

    def get_last_question(self) -> Union[str, list]:
        if self.__cache_json_data:
            return self.__cache_json_data[-1]["question"]
        else:
            assert False, f"{self.__cache_json_data}"

    def get_questions(self, num: int) -> Sequence[Sequence[Any]]:
        self.__fetch_raw_data(num)
        return self.__reformat_information()

    def __fetch_raw_data(self, num: int) -> None:
        self.__cache_json_data = self.__delegate.get_questions(num)

    # SKTODO rebuild to nametuple[MutSequese[Any]]
    def __reformat_information(self) -> Sequence[Sequence[Any]]:
        key_list: Sequence[str] = ["id", "question", "answer", "created_at"]
        return [[row[key] for row in self.__cache_json_data] for key in key_list]


class PostgresqlStorageService(StorageService):
    def __init__(self, schema="./yo1k/qaws/questions_schema.sql"):
        self.schema = schema
        self.__conn = psycopg.connect(
                dbname="postgres",
                user="postgres",
                password="postgres",
                # host="db",
                host="127.0.0.1",
                port="5432")
        self.__create_table()

    def __create_table(self):
        with self.__conn.transaction():
            with self.__conn.cursor() as cur:
                with open(self.schema) as schema:
                    cur.execute(schema.read())

    def insert_uniq_questions(self, data: list[list[Any]]) -> int:
        with self.__conn.cursor() as cur:
            cur.execute(
                    """
                    WITH inserted AS (
                        INSERT INTO questions
                        (SELECT * FROM unnest(
                                %s::int[],
                                %s::text[],
                                %s::text[],
                                %s::timestamptz[]))
                        ON CONFLICT (id) DO NOTHING
                        RETURNING id)
                    SELECT COUNT(*) FROM inserted;""",
                    data)
            returning: list[tuple[Any]] = cur.fetchall()
        return returning[0][0]

    def finalize(self) -> None:
        self.__conn.commit()
        self.__conn.close()


def init_flask_app_context():
    # config = app.config
    g.db_service = PostgresqlStorageService()
    g.client = JSONQuestionService()
    g.questions_service = DefaultQuestionService(g.client)


def create_app():
    app = Flask(__name__)

    app.before_request(init_flask_app_context)

    @app.teardown_request
    def final(e=None):
        print(f"{'questions_service' in g}")
        g.db_service.finalize()

    @app.route('/', methods=['POST'])
    def request_questions() -> Union[str, dict[None, None], None]:
        # if request.method == "POST":
        questions_num: int = request.get_json().get('questions_num')  # type: ignore
        assert isinstance(questions_num, int) and questions_num >= 0, \
            f"questions_num={questions_num}"
        if questions_num == 0:
            return {}
        else:
            prep_data = g.questions_service.get_questions(questions_num)
            retries: int = 100
            while retries > 0:
                retries -= 1
                fail_uniq_num = questions_num - g.db_service.insert_uniq_questions(prep_data)
                if fail_uniq_num:
                    prep_data = g.questions_service.get_questions(fail_uniq_num)
                else:
                    return g.questions_service.get_last_question()
            abort(500)

    return app
