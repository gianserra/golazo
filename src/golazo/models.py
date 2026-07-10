from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


Status = Literal["Planned", "Blocked", "Partial", "Done"]
LLMProvider = Literal["openai", "ollama", "lmstudio"]
ReasoningLevel = Literal["none", "minimal", "low", "medium", "high", "xhigh"]


class LLMConfig(BaseModel):
    provider: LLMProvider = "openai"
    model: str = Field(default="", max_length=200)
    reasoning: ReasoningLevel = "medium"


class GoalCreate(BaseModel):
    goal_id: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)


class FeatureCreate(BaseModel):
    feature_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=4000)
    status: Status = "Planned"


class StatusUpdate(BaseModel):
    status: Status


class StepCreate(BaseModel):
    step_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    title: str = Field(min_length=1, max_length=500)
    done: bool = False


class StepUpdate(BaseModel):
    done: bool


class SliceCreate(BaseModel):
    feature_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    summary: str = Field(min_length=1, max_length=4000)
    status: Status | None = None
    evidence: list[str] = Field(default_factory=list, max_length=50)


class CodexRunCreate(BaseModel):
    prompt: str = Field(min_length=1, max_length=20_000)
    goal_id: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    thread_id: str | None = Field(default=None, min_length=1, max_length=200)
    working_directory: str = "."
    sandbox: Literal["read-only", "workspace-write"] = "workspace-write"
    llm_config: LLMConfig | None = None


class WorkspaceSelect(BaseModel):
    path: str = Field(min_length=1, max_length=4096)


class ProjectCreate(BaseModel):
    path: str = Field(min_length=1, max_length=4096)
    activate: bool = True


class NameUpdate(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class ThreadUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=200)
    llm_config: LLMConfig | None = None
