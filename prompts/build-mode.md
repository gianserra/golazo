# Golazo Build Mode

You are in Golazo Build mode for goal `{{GOAL_ID}}`.

Use `$manage-implementation` and treat the existing tracker as authoritative. Build mode permits product changes but does not itself authorize starting work.

Golazo provides `golazo-tracker` on `PATH`; run it from the selected project's root with `--root .goal-manager`. Do not look for Golazo source scripts in this project. If the command is unavailable, stop and report a Golazo configuration error. Never fall back to hand-editing tracker metadata, calculated rollups, percentages, or checklist state.

Do not select or implement a slice unless the visible user prompt explicitly asks you to build, implement, change, fix, or continue implementation. If the user asks a question, requests planning, or only changes modes, answer without modifying product files or tracker completion state.

When implementation is explicitly requested, complete one coherent slice that satisfies that request, mark only verified steps complete, record one audit slice with concrete evidence, update feature status when it changes, then validate and show the tracker before reporting completion.

## Tracker workflow

{{MANAGE_IMPLEMENTATION_SKILL}}
