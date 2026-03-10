# Self-Audit & Enhance

You are the VDP Analytics quality engineer. Perform a full audit of the dashboard app and codebase, then implement improvements.

## Step 1: Read the master reference

Read `CLAUDE.md` at the project root. Every decision must align with the rules, schema, and data hierarchy defined there.

## Step 2: Scan the entire app

Read every file in `dashboard/` and `scripts/`. For each file, evaluate:

1. **Functionality gaps** — Are there TODO comments, placeholder data, or incomplete features?
2. **Performance** — Are there redundant DB queries, missing caching, or slow loops that should be vectorized?
3. **UX/Visual quality** — Are charts clear and labeled? Are KPI cards aligned? Is the color scheme consistent? Are there accessibility issues (contrast, labels)?
4. **Data accuracy** — Do queries match the schema in CLAUDE.md? Are metric units correct (occ as %, ADR in USD)?
5. **Error handling** — Are there bare `except:` blocks, missing null checks, or unhandled edge cases?
6. **Security** — Is the Anthropic API key server-side only? Are there any secrets in client-facing code?
7. **Code quality** — Type hints, naming conventions, dead code, duplicated logic?
8. **Interactive features** — Can charts be improved with tooltips, click interactions, drill-downs, or animations?
9. **AI features** — Can the Claude AI Analyst panel be enhanced with new prompt buttons, better context, or streaming responses?
10. **Mobile responsiveness** — Does the layout work on tablet/phone screens?

## Step 3: Present findings

Create a numbered list of every improvement opportunity found. Group by category (UX, Performance, Data, Security, AI, Code Quality). For each item:
- Describe the current state
- Describe the improvement
- Estimate impact (High / Medium / Low)

Present this list and wait for approval before implementing.

## Step 4: Implement approved changes

After the user confirms (or says "implement all"):
1. Make all changes across all affected files in a single pass.
2. Run `streamlit run dashboard/app.py` to verify the app starts without errors.
3. Run `python scripts/run_pipeline.py` to verify the pipeline still works.
4. Update `CLAUDE.md` → `## Update Log` with today's date and a summary of changes.
5. If any new lessons were learned during the audit, append them to `## Lessons Learned`.
6. Commit with message: `enhance: [summary of improvements]`
7. Push to `origin main`.

## Step 5: Report

Summarize what was changed, what was skipped (and why), and suggest the next round of improvements.

$ARGUMENTS
