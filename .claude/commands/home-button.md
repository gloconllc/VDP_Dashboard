# Add Home Button to Dashboard Title

In `dashboard/app.py`, ensure the dashboard title "VDP Analytics" in the header acts as a clickable home button that:

1. Resets the active tab to "Overview" (the first tab).
2. Clears any active filters or date range selections back to defaults.
3. Is styled as the app logo/title — not a separate button. Use `st.markdown()` with an anchor tag styled to look like the header, or use Streamlit's native navigation if available.
4. Works on both desktop and mobile.

After making the change:
- Verify the app runs: `streamlit run dashboard/app.py`
- Update `CLAUDE.md` → `## Update Log`
- Commit: `feat: add home button to dashboard title`

$ARGUMENTS
