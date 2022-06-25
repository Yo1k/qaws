import urllib.request
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional, Generic, TypeVar, Sequence, NamedTuple, MutableSequence, Union
from flask import Flask, abort, current_app
from flask import request
from psycopg import Connection
from psycopg_pool import ConnectionPool


T_co = TypeVar("T_co", covariant=True)
JSONType = dict[str, Any]


class PreparedQuestions(NamedTuple):
    id: MutableSequence[int]
    question: MutableSequence[str]
    answer: MutableSequence[str]
    created_at: MutableSequence[datetime]


class QuestionService(ABC, Generic[T_co]):
    @abstractmethod
    def get_questions(self, num: int) -> T_co:
        pass


class StorageService(ABC):
    @abstractmethod
    def insert_uniq_questions(self, questions: PreparedQuestions) -> int:
        pass


class JSONQuestionService(QuestionService[Sequence[JSONType]]):
    def __init__(self, url="https://jservice.io/api/random?count="):
        self.__url: str = url

    def get_questions(self, num: int) -> Sequence[JSONType]:
        with urllib.request.urlopen(url=f"{self.__url}{num}") as response:
            return json.loads(response.read())


class DefaultQuestionService(QuestionService[PreparedQuestions]):
    def __init__(self, delegate: QuestionService[Sequence[JSONType]]):
        self.__delegate: QuestionService[Sequence[JSONType]] = delegate

    def get_questions(self, num: int) -> PreparedQuestions:
        return DefaultQuestionService.__prepare_questions(
                self.__delegate.get_questions(num))

    @staticmethod
    def __prepare_questions(questions: Sequence[JSONType]) -> PreparedQuestions:
        prep_questions = PreparedQuestions([], [], [], [])
        for question in questions:
            prep_questions.id.append(question["id"])
            prep_questions.question.append(question["question"])
            prep_questions.answer.append(question["answer"])
            prep_questions.created_at.append(question["created_at"])
        return prep_questions


class PostgresqlStorageService(StorageService):
    def __init__(self, conn_pool: ConnectionPool):
        self.__conn_pool: ConnectionPool = conn_pool
        self.__conn: Connection = self.__conn_pool.getconn()
        self.__init_schema()

    def __init_schema(self) -> None:
        with self.__conn.transaction():
            with self.__conn.cursor() as cur:  # SKTODO remove cursor
                with current_app.open_resource('schema.sql') as schema:
                    cur.execute(schema.read())

    def insert_uniq_questions(self, questions: PreparedQuestions) -> int:
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
                    questions)
            returning: list[tuple[Any]] = cur.fetchall()
        return returning[0][0]

    def close(self) -> None:
        try:
            self.__conn.commit()
        finally:
            self.__conn_pool.putconn(self.__conn)


class QAWS:
    def __init__(self, conn_pool: ConnectionPool):
        # SKTODO add type hints
        self.__conn_pool: ConnectionPool = conn_pool
        # self.db_service: Optional[PostgresqlStorageService] = None
        # self.client: Optional[JSONQuestionService] = None
        # self.questions_service: Optional[DefaultQuestionService] = None
        self.db_service: Optional[PostgresqlStorageService] = PostgresqlStorageService(conn_pool)
        self.client = JSONQuestionService()
        self.questions_service = DefaultQuestionService(self.client)

    # def before_request(self) -> None:
    #     self.db_service = PostgresqlStorageService(self.__conn_pool)
    #     self.client = JSONQuestionService()
    #     self.questions_service = DefaultQuestionService(self.client)

    # SKTODO add type hints
    def request_questions(self) -> Union[str, dict]:
        questions_num: int = request.get_json().get('questions_num')
        assert isinstance(questions_num, int) and questions_num >= 0, \
            f"questions_num={questions_num}"
        if questions_num == 0:
            return {}
        else:
            questions = self.questions_service.get_questions(questions_num)
            retries: int = 100
            while retries > 0:
                retries -= 1
                fail_uniq_num = questions_num - self.db_service.insert_uniq_questions(questions)
                if fail_uniq_num:
                    questions = self.questions_service.get_questions(fail_uniq_num)
                    questions_num = fail_uniq_num
                else:
                    return questions.question[-1]
            abort(500)

    def close(self, e=None):  # SKTODO understand necessity of `e=None`
        self.db_service.close()


__pool = ConnectionPool(
        conninfo="dbname='postgres'"
                 "user='postgres'"
                 "password='postgres'"
                 "host='127.0.0.1'"
                 "port='5432'")


def create_app():
    app = Flask(__name__)
    qaws = QAWS(conn_pool=__pool)

    # app.before_request(qaws.before_request)  # SKTODO remove
    app.teardown_request(qaws.close)
    app.add_url_rule(
            rule="/",
            methods=['POST'],
            view_func=qaws.request_questions)

    return app

# SKTODO
# 1) change StorageService to using context manager for connection, transaction
# 2) add dependency injection to QAWS: PostgresqlStorageService, DefaultQuestionServiceб JSONQuestionService
# 3) add parameter `conn` to `request_questions`, `insert_uniq_questions`

# add to `readme.md`: `export FLASK_APP="yo1k.qaws.qaws_app:create_app()"`
