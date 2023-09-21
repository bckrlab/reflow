from ..recipe import Option, Options, Recipe


def format_options(
    options: Options, include_noop: bool = False, sort_steps: bool = True
):
    if sort_steps:
        steps = sorted(options.keys())
    else:
        steps = options.keys()

    return "___".join(
        [
            format_option(step, options[step])
            for step in steps
            if include_noop or options[step] != Recipe.NOOP
        ]
    )


def format_option(step, option):
    return f"{step}={option}"


def parse_options(options_string: str) -> Options:
    return dict([parse_branch(b) for b in options_string.split("___")])


def parse_branch(option_string: str) -> Option:
    return option_string.split("=")
