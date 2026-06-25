from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .paths import PROJECT_ROOT
from .settings import RuntimeSettings

DOCS_DIR = PROJECT_ROOT / "docs"

def export_pages_site(
    settings: RuntimeSettings,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    docs_dir = output_dir or DOCS_DIR
    docs_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Copy the SQLite database
    db_path = settings.database_path
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}. Run the pipeline first.")
    
    docs_db_path = docs_dir / "grandchase.db"
    shutil.copy2(db_path, docs_db_path)
    
    # 2. Copy python package files to docs/grandchase_meta_analyzer
    src_dir = PROJECT_ROOT / "src" / "grandchase_meta_analyzer"
    target_src_dir = docs_dir / "grandchase_meta_analyzer"
    target_src_dir.mkdir(parents=True, exist_ok=True)
    
    stlite_files = {
        "grandchase.db": {"url": "./grandchase.db"},
        "app.py": (
            "import sys\\n"
            "import typing_extensions\\n"
            "\\n"
            "# Patch missing TypeAliasType in typing_extensions for Altair compatibility in Stlite\\n"
            "if not hasattr(typing_extensions, 'TypeAliasType'):\\n"
            "    class TypeAliasType:\\n"
            "        def __init__(self, name, value, type_params=()):\\n"
            "            self.__name__ = name\\n"
            "            self.__value__ = value\\n"
            "            self.__type_params__ = type_params\\n"
            "        def __repr__(self):\\n"
            "            return f'TypeAliasType({self.__name__}, {self.__value__})'\\n"
            "    typing_extensions.TypeAliasType = TypeAliasType\\n"
            "\\n"
            "# Patch missing TypeIs in typing_extensions for Altair compatibility in Stlite\\n"
            "if not hasattr(typing_extensions, 'TypeIs'):\\n"
            "    if hasattr(typing_extensions, 'TypeGuard'):\\n"
            "        typing_extensions.TypeIs = typing_extensions.TypeGuard\\n"
            "    else:\\n"
            "        class _TypeIsMeta(type):\\n"
            "            def __getitem__(cls, item):\\n"
            "                return bool\\n"
            "        class TypeIs(metaclass=_TypeIsMeta):\\n"
            "            pass\\n"
            "        typing_extensions.TypeIs = TypeIs\\n"
            "\\n"
            "import streamlit as st\\n"
            "import grandchase_meta_analyzer.explorer_app as app\\n"
            "from pathlib import Path\\n"
            "app.DB_PATH = Path('grandchase.db')\\n"
            "app.main()"
        )
    }
    
    for py_file in src_dir.rglob("*.py"):
        if py_file.name == "__init__.py" and py_file.parent == src_dir:
            # We skip __init__.py because we'll auto-generate it or let stlite handle it? 
            # Actually we should just include it.
            pass
            
        rel_path = py_file.relative_to(src_dir)
        target_py_file = target_src_dir / rel_path
        target_py_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(py_file, target_py_file)
        
        # Add to stlite files mapping
        stlite_target = f"grandchase_meta_analyzer/{rel_path}"
        stlite_files[stlite_target] = {"url": f"./{stlite_target}"}
        
    # 3. Generate index.html
    index_html_content = f"""<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>GrandChase Atlas</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@stlite/mountable@0.39.0/build/stlite.css" />
  </head>
  <body>
    <div id="root"></div>
    <script src="https://cdn.jsdelivr.net/npm/@stlite/mountable@0.39.0/build/stlite.js"></script>
    <script>
      stlite.mount(
        {{
          requirements: ["altair<5.3.0", "pandas", "streamlit", "sqlite3"],
          entrypoint: "app.py",
          files: {json.dumps(stlite_files, indent=12)}
        }},
        document.getElementById("root"),
      )
    </script>
  </body>
</html>"""

    (docs_dir / "index.html").write_text(index_html_content, encoding="utf-8")
    (docs_dir / ".nojekyll").write_text("", encoding="utf-8")
    
    return {
        "docs_dir": str(docs_dir.relative_to(PROJECT_ROOT)),
        "db_size_mb": round(docs_db_path.stat().st_size / (1024 * 1024), 2),
        "files_copied": len(stlite_files) - 2
    }
