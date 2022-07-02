import urllib.request
import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Generic, TypeVar, Sequence, NamedTuple, MutableSequence, Union, Callable
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
    def insert_uniq_questions(self, questions: PreparedQuestions, conn: Connection) -> int:
        pass


class JSONQuestionService(QuestionService[Sequence[JSONType]]):
    def __init__(self, url: str = "https://jservice.io/api/random?count=") -> None:
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
    def insert_uniq_questions(self, questions: PreparedQuestions, conn: Connection) -> int:
        with conn.cursor() as cur:
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


class QAWS:
    def __init__(
            self,
            db_service: PostgresqlStorageService,
            questions_service: DefaultQuestionService):
        self.db_service: PostgresqlStorageService = db_service
        self.questions_service: DefaultQuestionService = questions_service

    def request_questions(self, conn: Connection) -> Union[str, dict]:
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
                fail_uniq_num = (
                        questions_num
                        - self.db_service.insert_uniq_questions(questions, conn=conn))
                if fail_uniq_num:
                    questions = self.questions_service.get_questions(fail_uniq_num)
                    questions_num = fail_uniq_num
                else:
                    return questions.question[-1]
            abort(500)


def with_tx_connection(pool: ConnectionPool, func: Callable):
    with pool.connection() as conn:
        return func(conn)


def init_schema(conn: Connection) -> None:
    with conn.cursor() as cur:
        with current_app.open_resource('schema.sql') as schema:
            cur.execute(schema.read())


def create_app():
    app = Flask(__name__)
    conn_pool = ConnectionPool(
            conninfo="dbname='postgres'"
                     "user='postgres'"
                     "password='postgres'"
                     "host='127.0.0.1'"
                     "port='5432'",
            open=False)
    db_service = PostgresqlStorageService()
    question_service = DefaultQuestionService(delegate=JSONQuestionService())
    qaws = QAWS(
            db_service=db_service,
            questions_service=question_service)

    app.before_first_request(f=lambda: (
            conn_pool.open()
            and with_tx_connection(pool=conn_pool, func=init_schema)))

    app.add_url_rule(
            rule="/",
            methods=['POST'],
            view_func=lambda: with_tx_connection(
                    pool=conn_pool,
                    func=qaws.request_questions))
    return app
