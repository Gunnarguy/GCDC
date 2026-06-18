## 2025-06-15 - Added Placeholders and Tooltips to Streamlit Sidebar Inputs
**Learning:** Streamlit's `st.text_input` benefits greatly from `placeholder` and `help` kwargs. While `placeholder` is immediately visible, `help` automatically adds a small "?" tooltip icon next to the input label, which is an excellent, native way to provide context without cluttering the UI. This is a highly reusable pattern for Streamlit apps.
**Action:** Always check if Streamlit inputs (`text_input`, `selectbox`, `multiselect`, etc.) can benefit from `help` tooltips or `placeholder` text to guide users without requiring extra `st.caption` or `st.markdown` elements.

## 2025-06-15 - Added Visual Icons to Sidebar Navigation
**Learning:** Adding relevant emoji icons to primary navigation elements (like `st.radio` options in a sidebar) significantly improves the scannability of the interface. Users can visually parse options faster than reading text labels alone. Streamlit's `format_func` in `st.radio` is an excellent mechanism to inject these icons without changing the underlying value list logic.
**Action:** Always consider prepending relevant icons to core navigation menus and tabs to make the UI feel more welcoming and easier to scan quickly.

## 2025-06-16 - Added Visual Icons to Empty States
**Learning:** Empty states rendered with `st.info` are very common in Streamlit apps to indicate missing data or filtering results. Adding a contextual `icon` parameter (like 🔍 for search, 👈 for selections, or 📭 for empty data) makes these messages significantly more visually distinct and scannable without adding custom CSS.
**Action:** Always check if Streamlit's `st.info`, `st.warning`, or `st.success` can benefit from an appropriate `icon` parameter to improve visual hierarchy and scannability, particularly for empty states.
