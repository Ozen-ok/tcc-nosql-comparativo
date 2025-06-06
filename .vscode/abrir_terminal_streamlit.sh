#!/bin/bash
echo "Iniciando o Streamlit..."
cd /mnt/c/Users/ozen/Desktop/tcc-nosql-comparativo
source .venv/bin/activate

streamlit run src/streamlit_app/app.py

# Mantém o terminal aberto após rodar, já dentro do ambiente e pasta certa
exec bash --rcfile <(echo "cd /mnt/c/Users/ozen/Desktop/tcc-nosql-comparativo; source .venv/bin/activate")

# chmod +x /mnt/c/Users/ozen/Desktop/tcc-nosql-comparativo/.vscode/abrir_terminal_1.sh
# # This script is used to open a terminal in the specified directory and activate a Python virtual environment.
# # It changes the current directory to /mnt/c/Users/ozen/Desktop/tcc-nosql-comparativo, activates the virtual environment located in .venv,
# # and then opens a new bash shell.
# # The last line makes the script executable.
# # To run this script, you can use the command:
# # ./abrir_terminal_1.sh
# # Make sure to replace the path with the correct one for your system.
# # Note: The script assumes that the virtual environment is located in the same directory as the script.
