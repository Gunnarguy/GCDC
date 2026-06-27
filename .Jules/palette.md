## 2025-06-25 - Ignored Pre-existing Test Failures
**Learning:** During my work, I encountered existing failures in `tests/test_normalize.py` that were unrelated to my UX improvements in `explorer_app.py`. My focus should remain strictly on the UX modifications without getting bogged down in fixing unrelated backend testing issues unless they specifically block my implementation.
**Action:** When running tests as part of the validation process, carefully assess if failures are regressions caused by my changes or pre-existing technical debt. Proceed with PR submission if the failures are unrelated to the current scope.
## 2025-06-15 - Added Placeholders and Tooltips to Streamlit Sidebar Inputs
**Learning:** Streamlit's `st.text_input` benefits greatly from `placeholder` and `help` kwargs. While `placeholder` is immediately visible, `help` automatically adds a small "?" tooltip icon next to the input label, which is an excellent, native way to provide context without cluttering the UI. This is a highly reusable pattern for Streamlit apps.
**Action:** Always check if Streamlit inputs (`text_input`, `selectbox`, `multiselect`, etc.) can benefit from `help` tooltips or `placeholder` text to guide users without requiring extra `st.caption` or `st.markdown` elements.

## 2025-06-15 - Added Visual Icons to Sidebar Navigation
**Learning:** Adding relevant emoji icons to primary navigation elements (like `st.radio` options in a sidebar) significantly improves the scannability of the interface. Users can visually parse options faster than reading text labels alone. Streamlit's `format_func` in `st.radio` is an excellent mechanism to inject these icons without changing the underlying value list logic.
**Action:** Always consider prepending relevant icons to core navigation menus and tabs to make the UI feel more welcoming and easier to scan quickly.

## 2025-06-17 - Empty State Visual Differentiation
**Learning:** Default informational banners (like Streamlit's `st.info`) for empty states can create a sterile user experience when repeated frequently across many data tables or search results. However, introducing completely custom HTML/CSS violates design constraints. Additionally, standard unicode emojis can render inconsistently or fail completely on Safari.
**Action:** Utilize built-in visual aids via Streamlit's native Material Symbols shortcodes (e.g. `icon=":material/search:"` for search, `icon=":material/menu_book:"` for database views). This improves UX and ensures pixel-perfect, cross-browser compatibility across Safari and Chrome without needing custom CSS.

## 2025-06-25 - Native constraints vs manual warnings in multiselect
**Learning:** For `st.multiselect`, trying to implement manual selection limits (e.g., checking `len(selected_items) > X`, cutting off extra items, and displaying a reactive warning) creates a worse UX. It allows invalid states briefly and requires the user to process an error message. Instead, using Streamlit's native `max_selections=X` parameter completely prevents the user from selecting more items than allowed, providing instant visual feedback by disabling remaining options. It's a much cleaner, more proactive accessibility and usability pattern.
**Action:** Always prefer native constraints (like `max_selections`) over reactive validation and warnings to prevent user errors before they happen.
