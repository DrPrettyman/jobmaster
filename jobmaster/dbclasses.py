import os
from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB, UUID, SMALLINT, BOOLEAN
from typing import List
from typing import Optional

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import DeclarativeBase


_schema = os.environ.get('JOBMASTER_SCHEMA', 'jobmaster')


class Base(DeclarativeBase):
    __table_args__ = {"schema": _schema}


class Task(Base):
    __tablename__ = 'tasks'

    id: Mapped[str] = mapped_column(primary_key=True)
    type_key: Mapped[str]
    task_key: Mapped[str]
    help: Mapped[Optional[str]]
    process_units: Mapped[int]
    write_all: Mapped[List[str]]
    parameters: Mapped["TaskParameter"] = relationship(back_populates='task')
    dependencies: Mapped[List["Dependency"]] = relationship(back_populates='task')

    def __repr__(self):
        return f"<Task {self.key}>"


class TaskParameter(Base):
    __tablename__ = "task_parameters"

    task_id: Mapped[str] = mapped_column(ForeignKey(_schema+".tasks.id"))
    parameter_key: Mapped[str]
    value_type: Mapped[str]
    select_from: Mapped[List[JSONB]]
    default_value: Mapped[Optional[JSONB]]
    required: Mapped[BOOLEAN]
    write_all: Mapped[BOOLEAN]
    help: Mapped[Optional[str]]

    task: Mapped["Task"] = relationship(back_populates="task_parameters")

    def __repr__(self):
        return f"<TaskParameter {self.task_id}: {self.parameter_key}>"


class Dependency(Base):
    __tablename__ = "dependencies"

    task_id: Mapped[str] = mapped_column(ForeignKey(_schema+".tasks.id"))
    dependency_id: Mapped[str] = mapped_column(ForeignKey(_schema+".tasks.id"))
    hours: Mapped[Optional[float]]
    parameters: Mapped[List["DependencyParameter"]] = relationship(back_populates='dependency')

    task: Mapped["Task"] = relationship(back_populates="dependencies")
    dependency: Mapped["Task"] = relationship(back_populates="dependencies")

    def __repr__(self):
        return f"<Dependency {self.task_id} -> {self.dependency_id}>"