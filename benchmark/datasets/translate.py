import itertools
import re
from pathlib import Path
from typing import Callable, Optional


DEFAULT_CHUNK_SIZE = 100000


def max_digits(dataset_partition_filenames):
    return max(
        map(
            lambda name: len(Path(name).stem.split("_")[1]), dataset_partition_filenames
        )
    )


def get_normalized_integer(number: int, max_nb_digits: int) -> str:
    return ("{:0" + str(max_nb_digits) + "d}").format(number)


def get_normalized_integer_alt(number: int, max_nb: int) -> str:
    return get_normalized_integer(number, len(str(max_nb)))


def from_str_to_int_with_label(arg: str) -> int:
    pattern = r"([0-9]+)(m|k)?"
    match = re.match(pattern, arg)
    if match is None:
        raise ValueError(f"pattern '{pattern}' not recognized with input: '{arg}'")
    number = match.group(1)
    power_str = match.group(2)
    power = 6 if power_str == "m" else 3 if power_str == "k" else 0
    return int(number) * 10**power


def quote_csv_line(line: str):
    line = line.strip()
    tokens = line.split(",")
    for i, token in enumerate(tokens):
        # if already double-quoted, don't quote
        if token and token[0] == '"' and token[-1] == '"':
            assert '"' not in token[1:-1], f"quotes are not allowed for {line}"
            tokens[i] = token
        else:
            assert '"' not in token, f"quotes are not allowed for {line}"
            tokens[i] = f'"{token}"'
    return ",".join(tokens)


def quote_csv_file(input_content: str) -> str:
    return "\n".join(map(quote_csv_line, input_content.splitlines(keepends=False)))


def transform_dataset_file(input_file: Path, output_file: Path, predicate_name: str):
    lines = input_file.read_text().splitlines(keepends=False)
    atoms = map(lambda line: f"{predicate_name}(" + quote_csv_line(line) + ").", lines)
    output_file.write_text("\n".join(atoms))


def normalize_name(file_name: str, min_digits: int):
    dataset_name, nb_rows = Path(file_name).stem.split("_")
    new_nb_rows = (min_digits - len(nb_rows)) * "0" + nb_rows
    return f"{dataset_name}_{new_nb_rows}"


def transform_dataset_file_with_header(
    input_file: Path,
    output_file: Path,
    header: Optional[str] = None,
    predicate_name: Optional[str] = None,
    row_processor: Callable = lambda x: x,
    skip_lines: int = 0,
    size: Optional[int] = None,
):
    assert predicate_name is not None
    with input_file.open() as input_file_object, output_file.open(
        mode="w"
    ) as output_file_object:
        if header is not None:
            output_file_object.write(header + "\n")
        end_slice = None if size is None else skip_lines + size
        input_file_object = itertools.islice(input_file_object, skip_lines, end_slice)
        atoms = map(
            lambda line: f"{predicate_name}("
            + quote_csv_line(row_processor(line))
            + ").\n",
            input_file_object,
        )
        for chunk in itertools.islice(atoms, DEFAULT_CHUNK_SIZE):
            output_file_object.writelines(chunk)


def write_lines_for_vadalog(
    input_file: Path,
    output_file: Path,
    header: Optional[str] = None,
    predicate_name: Optional[str] = None,
    row_processor: Callable = lambda x: x,
    skip_lines: int = 0,
    size: Optional[int] = None,
):
    with input_file.open() as input_file_object, output_file.open(
        mode="w"
    ) as output_file_object:
        if header is not None:
            output_file_object.write(header + "\n")
        end_slice = None if size is None else skip_lines + size
        input_file_rows = itertools.islice(input_file_object, skip_lines, end_slice)
        processed_input_file_rows = map(row_processor, input_file_rows)
        for chunk in itertools.islice(processed_input_file_rows, DEFAULT_CHUNK_SIZE):
            output_file_object.writelines(chunk)


def normalize(line: str, nb_https: int = 2):
    assert nb_https > 0
    line = line.strip()
    match = re.search(",".join(["(http.*)"] * nb_https), line)
    groups = [match.group(i + 1) for i in range(nb_https)]
    normalized_groups = [group.replace(",", "_") for group in groups]
    new_line = ",".join(normalized_groups)
    return new_line + "\n"


def normalize_person_dataset_row(line: str):
    line = line.strip()
    match = re.search("(.*),.*,.*,.*,.*", line)
    new_line = match.group(1)
    return new_line.replace(",", "_") + "\n"


def get_nb_columns_from_csv(input_file: Path) -> int:
    return len(input_file.read_text().split("\n", maxsplit=1)[0].split(","))


def process_line_for_dlve(line: str) -> str:
    check_output = re.match('@output\("(.*)"\)', line)
    if check_output is not None:
        # to be processed later
        return line

    head, body = line.split(":-")
    head = head.strip()
    body = body.strip()
    variable_regex = " *[a-zA-Z0-9_]+\((.*)\)"

    head_variables_string = re.search(variable_regex, head).group(1)
    head_variables_string = re.sub(" +", "", head_variables_string)
    head_variables = set(head_variables_string.split(","))

    body_variables = set()
    for body_atom in re.findall(" *[a-zA-Z0-9_]+\(.*?\)", body):
        body_atom = body_atom.strip()
        body_atom_variables_string = re.search(variable_regex, body_atom).group(1)
        body_atom_variables_string = re.sub(" +", "", body_atom_variables_string)
        body_atom_variables = set(body_atom_variables_string.split(","))
        body_variables.update(body_atom_variables)

    existentially_quantified_vars = head_variables.difference(body_variables)

    if existentially_quantified_vars:
        dlve_exist_prefix = (
            f"#exists{{{','.join(sorted(existentially_quantified_vars))}}}"
        )
        line = dlve_exist_prefix + line
    return line


def process_program_for_vadalog(input_file: str, *_args) -> str:
    input_file = re.sub("%.*\n", "", input_file)
    input_file = re.sub("@(bind|mapping|input).*\n", "", input_file)
    # input_file = _transform_multiline_rules_in_one_line(input_file)
    return input_file


def process_program_for_vadalog_set_query(input_file: str, query_name: str) -> str:
    """Transform an output rule into a query rule (i.e. using ':-')."""
    input_file = re.sub("%.*\n", "", input_file)
    input_file = re.sub("@(bind|mapping|input).*\n", "", input_file)
    input_file = _transform_multiline_rules_in_one_line(input_file)
    input_file = re.sub(f"{query_name}(.*) *:-", f"{query_name}\g<1> ?-", input_file)
    return input_file


def process_chasebench_query_file(chasebench_query_file: str):
    content = chasebench_query_file
    content = re.sub("\?[a-zA-Z0-9_]+(?= *[,)])", lambda m: m.group(0).upper()[1:], content)
    # one-line query
    content = content.replace("\n", "")
    content = content.replace("<-", "?-")
    return content


def process_program_for_vadalog_with_original_query(input_file: str, chasebench_query_file: str) -> str:
    input_file = re.sub("%.*\n", "", input_file)
    input_file = re.sub("@(bind|mapping|input).*\n", "", input_file)
    # remove old queries
    input_file = _transform_multiline_rules_in_one_line(input_file)
    input_file = re.sub("^q[0-9]+.*\n", "", input_file, flags=re.MULTILINE)
    # parse new query from chasebench format
    query_line = process_chasebench_query_file(chasebench_query_file)

    input_file = input_file + "\n" + query_line

    return input_file


def _transform_multiline_rules_in_one_line(input_file: str):
    tmp = input_file
    new_file = ""
    while (match := re.search("(.*) *\n?:-\n? *\n?(.*, *\n)* *(.*)\.", tmp)) is not None:
        pos, endpos = match.span()
        rule = match.group(0).replace("\n", "")
        rule = re.sub(":- *([A-Za-z])", ":- \g<1>", rule)
        rule = re.sub("\),( *)([A-Za-z])", "), \g<2>", rule)

        # update new file
        new_file = new_file + tmp[:pos] + rule
        tmp = tmp[endpos:]
    if new_file == "":
        new_file = input_file
    else:
        new_file = new_file + tmp
    return new_file


def process_program_for_dlve(input_file: str, *_args) -> str:
    input_file = re.sub("%.*\n", "", input_file)
    input_file = re.sub("@(bind|mapping|input).*\n", "", input_file)
    input_file = _transform_multiline_rules_in_one_line(input_file)

    lines = input_file.splitlines(keepends=False)
    lines = [line for line in lines if line.strip()]
    new_lines = map(process_line_for_dlve, lines)
    output = "\n".join(new_lines)

    # find output predicates
    output_statements = sorted(set(re.findall('@output\("(.*)"\)', output)))
    if len(output_statements) == 0:
        return output

    assert len(output_statements) == 1
    predicate_name = output_statements[0]
    # find output predicate in the program
    finditer = re.finditer(f" *(#exists{{(.*?)}})? *{predicate_name}\((.*?)\)", output)
    output_match = next(finditer)
    _, exist_variables_string, variables_string = output_match.groups()
    variables = variables_string.split(",")
    nb_variables = len(variables)
    id_to_new_var = [f"X{i}" for i in range(nb_variables)]
    new_variables_string = ",".join(id_to_new_var)
    new_exist_clause = ""
    if exist_variables_string:
        exist_vars = exist_variables_string.split(",")
        exist_positions = [i for i in range(nb_variables) if variables[i] in exist_vars]
        new_exist_variable_string = ",".join(
            map(lambda vid: id_to_new_var[vid], exist_positions)
        )
        new_exist_clause = f"#exists{{{new_exist_variable_string}}}"
    dlve_query = f"{new_exist_clause}{predicate_name}({new_variables_string})?"
    # remove old output statement
    output = re.sub("@.*\n?", "", output)
    # add new line
    output += "\n" + dlve_query

    return output
