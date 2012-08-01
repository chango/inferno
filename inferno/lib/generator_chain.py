
# The generator_chain is used to support the part_preprocess and parts_postprocess.  The function is a
# generator that takes an iteraotr over a list of arbitrary values, a list list of function generators,
# and a kwargs for additional data to be passed to the generator functions.
#
# generator_chain will execute all of the func_list functions passing results from the previous functions,
# creating a chain of generators
def generator_chain(it, func_list, **kwargs):
    def _apply_process(it, func):
        for val in it:
            for rval in func(val, **kwargs):
                yield rval

    for func in func_list:
        it = _apply_process(it, func)
    for rval in it:
        yield rval

