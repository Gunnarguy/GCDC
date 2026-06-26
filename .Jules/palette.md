## 2025-06-15 - Added Placeholders and Tooltips to Streamlit Sidebar Inputs
**Learning:** Streamlit's `st.text_input` benefits greatly from `placeholder` and `help` kwargs. While `placeholder` is immediately visible, `help` automatically adds a small "?" tooltip icon next to the input label, which is an excellent, native way to provide context without cluttering the UI. This is a highly reusable pattern for Streamlit apps.
**Action:** Always check if Streamlit inputs (`text_input`, `selectbox`, `multiselect`, etc.) can benefit from `help` tooltips or `placeholder` text to guide users without requiring extra `st.caption` or `st.markdown` elements.

## 2025-06-15 - Added Visual Icons to Sidebar Navigation
**Learning:** Adding relevant emoji icons to primary navigation elements (like `st.radio` options in a sidebar) significantly improves the scannability of the interface. Users can visually parse options faster than reading text labels alone. Streamlit's `format_func` in `st.radio` is an excellent mechanism to inject these icons without changing the underlying value list logic.
**Action:** Always consider prepending relevant icons to core navigation menus and tabs to make the UI feel more welcoming and easier to scan quickly.

## 2025-06-17 - Empty State Visual Differentiation
**Learning:** Default informational banners (like Streamlit's `st.info`) for empty states can create a sterile user experience when repeated frequently across many data tables or search results. However, introducing completely custom HTML/CSS violates design constraints. Additionally, standard unicode emojis can render inconsistently or fail completely on Safari.
**Action:** Utilize built-in visual aids via Streamlit's native Material Symbols shortcodes (e.g. `icon=":material/search:"` for search, `icon=":material/menu_book:"` for database views). This improves UX and ensures pixel-perfect, cross-browser compatibility across Safari and Chrome without needing custom CSS.

## 2025-06-21 - Enforcing limits via native Multiselect parameters
**Learning:** Preventing users from making an error is better UX than warning them after they make it. In Streamlit's `st.multiselect`, trying to handle max item limits manually with warning boxes creates unnecessary friction.
**Action:** Always use the built-in `max_selections` argument for `st.multiselect` to strictly prevent users from over-selecting, instead of doing length checks on the returned list and showing `st.warning` messages.

## 2025-06-21 - Activating Placeholders in Selectboxes
**Learning:** Streamlit's `st.selectbox` allows a `placeholder` argument, but it is invisible by default because `selectbox` implicitly sets `index=0` (selecting the first item).
**Action:** In order for a `st.selectbox` placeholder to actually be rendered to the user, you must explicitly set `index=None` when calling the component.
