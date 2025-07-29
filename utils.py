def activate_widget(*args):
    for i in args:
        i['state'] = 'normal'


def disable_widget(*args):
    for i in args:
        i['state'] = 'disabled'
