# Used as utility thingy

_in_root() {
    if [ -d ./luminadb ]; then
        return 0;
    fi
    return 1
}

err(){
    printf "[\033[31mERR\033[0m  ] $@\n"
}

_check_inroot(){
    if ! _in_root; then
        err "Not in root directory!"
        return 1
    fi
}

info(){
    printf "[\033[32mINFO\033[0m ] $@\n"
}

warn(){
    printf "[\033[33mWARN\033[0m ] $@\n"
}


tell(){
    printf "\033[32m->\033[0m $@\n"
}

ask(){
    printf "\033[32m?\033[0m $@" >&2
    read -r inputted
    printf "$inputted"
}

yes-no(){
    output="$(ask)"
    if [ "${output::1}" = "y" ]; then
        return 0
    fi
    return 1
}

check-result(){
    # $1 -> Status code
    # $2 -> Suite name/program name
    if [ "$1" == "0" ]; then
        info "$2 was successful :3"
    else
        err "$2 was failed :("
    fi
    return $1
}

if [ ! "$VIRTUAL_ENV" == "" ]; then
    if ! _check_inroot; then
        return 1
    fi
    source ./.venv/bin/activate
    source ~/.bashrc # In case it messed up your prompt
fi

run-pylint() {
    info "Running pylint..."
    [ ! _check_inroot ] && return 1
    if [ "$1" == "" ]; then
        src="./luminadb"
    else
        src="$1"
    fi
    pylint --rcfile ./dev-config/pylint.toml "$src" --output-format json2 > ./logs/pylint.log.json
    ./.venv/bin/python ./bin/summarize-pylint.py ./logs/pylint.log.json
    check-result $? pylint
}

run-black(){
    info "Running black..."
    [ ! _check_inroot ] && return 1
    if [ "$1" == "" ]; then
        src="./luminadb"
    else
        src="$1"
    fi
    black --config ./dev-config/black.toml "$src"
    check-result $? black
}

run-pytest(){
    info "Running pytest"
    [ ! _check_inroot ] && return 1
    if [ "$1" == "" ]; then
        src="./luminadb"
    else
        src="$1"
    fi
    pytest --config-file ./dev-config/pytest.ini -q --rootdir .
    check-result $? pytest
}

run(){
    # Run a program.
    # $1 -> Suite name
    # $2 -> Program args
    info "Running $1"
    [ ! _check_inroot ] && return 1
    $2 > ./logs/"$1.log"
    check-result $? "$1"
}
