import os
from datetime import datetime

def compile_project():
    root_dir = "."
    
    # Dynamically grab the name of the current folder to name the output
    project_name = os.path.basename(os.path.abspath(root_dir))
    if not project_name:
        project_name = "universal_project"
        
    output_file = f"{project_name}_compiled_system.txt"
    
    # A comprehensive list of extensions
    valid_exts = {
        # Web / JS Ecosystem
        ".js", ".ts", ".tsx", ".jsx", ".mjs", ".cjs", ".html", ".css", ".scss",
        # Backend / Systems / Scripts
        ".py", ".java", ".c", ".cpp", ".h", ".hpp", ".go", ".rs", ".rb", ".php", ".sh", ".bat", ".ps1",
        # Config / Data / Docs
        ".json", ".yaml", ".yml", ".xml", ".toml", ".ini", ".sql", ".http", ".txt"
    }
    
    # Common directories to ignore across various tech stacks
    ignore_dirs = {
        ".git", ".svn", ".hg",                           # Version control
        "node_modules", "vendor", "packages",            # Dependencies
        "venv", ".venv", "env", "__pycache__", ".tox",   # Python environments
        "dist", "build", "target", "out", ".next",       # Build outputs
        ".nuxt", "bin", "obj",
        ".idea", ".vscode", ".eclipse",                  # IDE settings
        ".gemini", "scratch", "tmp", "temp", "logs"      # Temp/misc files
    }
    
    # Specific files to explicitly ignore
    ignore_files = {
        "package-lock.json",
        "yarn.lock", 
        "pnpm-lock.yaml",
        "firebase-service-account.json"
    }
    
    always_include = {"Dockerfile", "Makefile", "requirements.txt", "Gemfile"}

    with open(output_file, "w", encoding="utf-8") as out_f:
        out_f.write(f"{project_name} - Compiled System Files\n")
        out_f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out_f.write("="*80 + "\n\n")
        
        # --- NEW: Structure Overview Generation ---
        out_f.write("=== Project Structure Overview ===\n")
        
        files_to_compile = []
        
        for root, dirs, files in os.walk(root_dir):
            # Modify dirs in-place to skip ignored directories
            dirs[:] = [d for d in dirs if d not in ignore_dirs]
            
            # Identify relative path for tree indentation
            rel_path = os.path.relpath(root, root_dir)
            indent_level = 0 if rel_path == "." else rel_path.count(os.sep) + 1
            indent = "  " * indent_level
            
            # Determine if folder is logically empty (after ignores)
            is_folder_empty = " [EMPTY]" if not dirs and not files else ""
            folder_name = project_name if rel_path == "." else os.path.basename(root)
            out_f.write(f"{indent}📁 Folder: {folder_name}{is_folder_empty}\n")
            
            for file in files:
                file_path = os.path.join(root, file)
                ext = os.path.splitext(file)[1].lower()
                
                # Skip self-compilation
                if os.path.basename(file_path) == os.path.basename(__file__) or file == output_file:
                    continue
                
                # --- EXCLUSION LOGIC ---
                if file.startswith(".env") or file in ignore_files:
                    continue
                    
                if "secret" in file.lower() or ext in {".pem", ".key", ".cert", ".p12", ".p8"}:
                    continue
                # -----------------------
                
                # Check if file is size 0 on disk
                is_file_empty = " [EMPTY]" if os.path.getsize(file_path) == 0 else ""
                out_f.write(f"{indent}  📄 File/Script: {file}{is_file_empty}\n")
                
                # Add to queue if it's a valid extension or always included
                if ext in valid_exts or file in always_include:
                    files_to_compile.append(file_path)
                    
        out_f.write("\n" + "="*80 + "\n\n")
        
        # --- EXISTING: Content Compilation ---
        out_f.write("=== File Contents ===\n\n")
        
        for file_path in files_to_compile:
            try:
                with open(file_path, "r", encoding="utf-8") as in_f:
                    content = in_f.read()
                
                out_f.write(f"=== File: {os.path.normpath(file_path)} ===\n")
                
                # Additional check during read phase
                if not content.strip():
                    out_f.write("[File is empty]\n\n")
                else:
                    out_f.write(content)
                    out_f.write("\n\n")
                    
            except UnicodeDecodeError:
                # Skip binary files that accidentally matched an extension
                out_f.write(f"=== File: {os.path.normpath(file_path)} ===\n")
                out_f.write("[Error: Binary or non-UTF-8 file skipped]\n\n")
            except Exception as e:
                out_f.write(f"=== File: {os.path.normpath(file_path)} ===\n")
                out_f.write(f"[Error reading file: {e}]\n\n")
                        
    print(f"Compilation complete. Structure mapped and code output saved to {os.path.abspath(output_file)}")

if __name__ == "__main__":
    compile_project()
