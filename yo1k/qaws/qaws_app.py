import atexit
import json
import urllib.request
from abc import ABC, abstractmethod
from datetime import datetime
from typing import (Any, Callable, Generic, MutableSequence, NamedTuple, Optional, Sequence,
                    TypeVar, Union)

from flask import Flask, abort, current_app, g, request
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
    def insert_uniq_questions(
            self, conn: Connection[tuple[Any]], questions: PreparedQuestions
    ) -> int:
        pass


class TransactionManager(ABC):
    @abstractmethod
    def do_in_default_tx(
            self,
            func: Callable[..., T_co],
            *args: Any,
            **kwargs: Any) -> T_co:
        pass


class JSONQuestionService(QuestionService[Sequence[JSONType]]):
    def __init__(self, url: str = "https://jservice.io/api/random?count=") -> None:
        self.__url: str = url

    def get_questions(self, num: int) -> Sequence[JSONType]:
        with urllib.request.urlopen(url=f"{self.__url}{num}") as response:
            questions: Sequence[JSONType] = json.loads(response.read())
            return questions


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


class DefaultTransactionManager(TransactionManager):
    def do_in_default_tx(
            self,
            func: Callable[..., T_co],
            *args: Any,
            **kwargs: Any) -> T_co:
        return func(conn=g.conn, *args, **kwargs)


class PgStorageService(StorageService):
    def insert_uniq_questions(self, conn: Connection[tuple[Any]], questions: PreparedQuestions) -> \
            int:
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
            inserted_count: int = returning[0][0]
            return inserted_count


class QAWS:
    def __init__(
            self,
            tx_manager: TransactionManager,
            db_service: StorageService,
            questions_service: QuestionService[PreparedQuestions]):
        self.tx_manager: TransactionManager = tx_manager
        self.db_service: StorageService = db_service
        self.questions_service: QuestionService[PreparedQuestions] = questions_service

    def request_questions(self) -> Union[str, dict[None, None]]:
        questions_num: int = request.get_json().get('questions_num')  # type: ignore
        assert isinstance(questions_num, int) and questions_num >= 0, \
            f"questions_num={questions_num}"
        if questions_num == 0:
            return {}
        else:
            questions = self.questions_service.get_questions(questions_num)
            retries: int = 100
            while retries > 0:
                retries -= 1
                inserted_questions_num = self.tx_manager.do_in_default_tx(
                        func=self.db_service.insert_uniq_questions,
                        questions=questions)
                fail_uniq_num = questions_num - inserted_questions_num
                if fail_uniq_num:
                    questions = self.questions_service.get_questions(fail_uniq_num)
                    questions_num = fail_uniq_num
                else:
                    return questions.question[-1]
            abort(500)


def init_schema(conn: Connection[tuple[Any]]) -> None:
    with conn.cursor() as cur:
        with current_app.open_resource('schema.sql') as schema:
            cur.execute(schema.read())


def create_app() -> Flask:
    app = Flask(__name__)
    conn_pool = ConnectionPool(
            conninfo="dbname='postgres'"
                     "user='postgres'"
                     "password='postgres'"
                     "host='db'"
                     "port='5432'",
            open=False)
    tx_manager = DefaultTransactionManager()
    db_service = PgStorageService()
    question_service = DefaultQuestionService(delegate=JSONQuestionService())
    qaws = QAWS(
            tx_manager=tx_manager,
            db_service=db_service,
            questions_service=question_service)

    with app.app_context():
        conn_pool.open()
        atexit.register(conn_pool.close)
        with conn_pool.connection() as conn:
            init_schema(conn)

    def get_conn() -> None:
        g.conn = conn_pool.getconn()

    def close_conn(e: Optional[BaseException] = None) -> None:  # pylint: disable=C0103, W0613
        conn = g.pop("conn", None)
        if conn is not None:
            conn.commit()
            conn_pool.putconn(conn)

    app.before_request(get_conn)
    app.add_url_rule(
            rule="/",
            methods=['POST'],
            view_func=qaws.request_questions)
    app.teardown_request(close_conn)
    return app
