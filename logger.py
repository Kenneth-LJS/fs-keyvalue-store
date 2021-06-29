import inspect
from inspect import Parameter

def print_log(func):

    def get_unused_var_name(var_name, existing_var_names):
        index = 0
        new_var_name = var_name
        while new_var_name in existing_var_names:
            index += 1
            new_var_name = f'{var_name}_{index}'
        return new_var_name

    global_context = {}
    new_func_name = 'func' if func.__name__ == '<lambda>' else func.__name__
    old_func_name = f'wrapped_{new_func_name}'
    
    existing_var_names = set([new_func_name, old_func_name])
    global_context[old_func_name] = func

    func_param_name_lst = [] # parameters
    func_params_lst = [] # parameters
    func_args_lst = [] # arguments

    function_params = inspect.signature(func).parameters
    for param_name, param in list(function_params.items()):
        param = param.replace(annotation=Parameter.empty)

        func_param_name_lst.append(param_name)

        if param.default != Parameter.empty and hasattr(param.default, '__name__'):
            # print(param_name, param.default, param.default.__name__, str(param))
            global_context[param.default.__name__] = param.default
            existing_var_names.add(param.default.__name__)
            func_params_lst.append(f'{param_name}={param.default.__name__}')
        else:
            func_params_lst.append(str(param))

        if param.kind == Parameter.VAR_POSITIONAL:
            func_args_lst.append(f'*{param_name}')
        elif param.kind == Parameter.VAR_KEYWORD:
            func_args_lst.append(f'**{param_name}')
        elif param.kind == Parameter.KEYWORD_ONLY:
            func_args_lst.append(f'{param_name}={param_name}')
        else:
            func_args_lst.append(param_name)

    self_name = list(function_params.keys())[0]
    func_params_str = ', '.join(func_params_lst)
    func_args_str = ', '.join(func_args_lst)

    log_str = ', '.join(f'{func_arg_str}={{repr({func_param_name})}}' for (func_arg_str, func_param_name) in zip(func_args_lst, func_param_name_lst))

    is_func_async = inspect.iscoroutinefunction(func)

    result_name = get_unused_var_name('result', existing_var_names)
    existing_var_names.add(result_name)

    exception_obj_name = get_unused_var_name('ex', existing_var_names)
    existing_var_names.add(exception_obj_name)

    log_prefix = f'[{new_func_name}] '

    func_str_lst = []
    func_str_lst.append(f'{"async " if is_func_async else ""}def {new_func_name}({func_params_str}):')
    func_str_lst.append(f'    print(f"{log_prefix}Called with args: {log_str}")')
    func_str_lst.append(f'    try:')
    func_str_lst.append(f'        {result_name} = {"await " if is_func_async else ""}{old_func_name}({func_args_str})')
    func_str_lst.append(f'    except Exception as {exception_obj_name}:')
    func_str_lst.append(f'        print(f"{log_prefix}Exception: {{repr({exception_obj_name})}}")')
    func_str_lst.append(f'        raise {exception_obj_name}')
    func_str_lst.append(f'    ')
    func_str_lst.append(f'    print(f"{log_prefix}Result: {{repr({result_name})}}")')
    func_str_lst.append(f'    return {result_name}')

    func_str = '\n'.join(func_str_lst)

    # print(func_str)

    local_context = {}
    exec(func_str, global_context, local_context)

    wrapped_function = local_context[new_func_name]

    return wrapped_function


def make_wrapped_log_function_do_log_str(
    self_name: str,
    logger_attribute_name: str,
    init_log_class_name: str,
    log_class: type,
    log_context_strs: list[str],
    indent='    '
):
    log_context_strs = [line for line in log_context_strs if len(line) > 0]

    if len(log_context_strs) == 0:
        return f'{indent}{self_name}.{logger_attribute_name}({init_log_class_name}({log_class}))'

    param_lines = [f'{indent}    {line},' for line in [log_class.__name__, *log_context_strs] if len(line) > 0]
    lines = []
    lines.append(f'{indent}{self_name}.{logger_attribute_name}({init_log_class_name}(')
    lines += param_lines
    lines.append(f'{indent}))')

    return '\n'.join(lines)