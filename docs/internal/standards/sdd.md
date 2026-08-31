# Spec-driven development

**Family:** Process
**Read when:** Specifying work, decomposing a milestone, writing acceptance criteria, or when a spec turns out to be wrong.

1. **Nothing is implemented before it is specified.** A task without acceptance
   criteria is not ready to start.
2. **Every task cites a requirement ID** from `../product/requirements.md`. Work
   serving no FR/NFR is unjustified.
3. **One task equals one commit equals one change that leaves the tree green.**
4. **Task IDs are stable and never reused.** Format `M<n>.<k>`.
5. **Only the current milestone is decomposed in detail.** Tasks written three
   milestones ahead are wrong by the time they are reached.
6. **Acceptance criteria are checkable by something other than an opinion** — a
   test, a gate, a number inside a bound. No gate here.
7. **A spec is reviewed before code is written.** A wrong spec produces correct
   code solving the wrong problem, and every downstream gate passes.
8. **A spec that turns out to be wrong is amended, not worked around.** If the
   correction changes an approach or a requirement, it is an ADR.
9. **Every spec states its cost impact** — which of rules R1–R18 (and R1b) in `cost.md` it
   touches and what the budget becomes. "None" is a valid answer; silence is not.
10. **Consult `docs/research/` before designing.** Contradicting a research
    conclusion is allowed; doing it without saying so is not.
