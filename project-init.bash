# Initialize development project directory

if [ ! -d luminadb ]; then
    printf "\033[31mError\033[0m: Not in project root directory!\n"
    exit 1
fi

source ./bin/include/utility.bash

pyv="$(ask 'What python version do you use? ')"
python="python$pyv"

info "Installing development dependency"
if [ ! -d ./.venv ]; then
    run "Python Virtual Environment" "$python -m venv .venv"
    [ -d ./.venv/bin ] && source .venv/bin/activate
    [ -d ./.venv/Scripts ] && source .venv/Scripts/activate
    run "Development Apps" "pip install -r ./dev-requirements.txt"
else
    info "Found venv dir, skipping."
fi

info "Creating some necessary directories"
for i in logs records; do
    [ ! -d "$i" ] && mkdir "$i"
done

info "Installing some files..."
./bin/install.bash

info "Project installed~"
