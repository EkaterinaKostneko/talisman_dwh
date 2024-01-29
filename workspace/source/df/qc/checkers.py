"""
Created on Apr 20, 2022

@author: pymancer@gmail.com
"""
def must_be_not_null(checker, ctx):
    return f'{ctx.df_schema_}{checker.name}({checker.parameters.attribute}) is false'


def must_be_shorter_than(checker, ctx):
    return f"""{ctx.df_schema_}{checker.name}({checker.parameters.attribute},
 {checker.parameters.max_length}) is false"""


def must_be_longer_than(checker, ctx):
    return f"""{ctx.df_schema_}{checker.name}({checker.parameters.attribute},
 {checker.parameters.min_length}) is false"""


def must_be_less_than(checker, ctx):
    return f"""{ctx.df_schema_}{checker.name}({checker.parameters.attribute},
 {checker.parameters.max_value}) is false"""


def must_be_more_than(checker, ctx):
    return f"""{ctx.df_schema_}{checker.name}({checker.parameters.attribute},
 {checker.parameters.min_value}) is false"""


def max_repeats(checker, ctx):
    times = checker.parameters.get('times', 1)
    attr = checker.parameters.attribute

    return f"""count({attr}) over (partition by {attr}) > {times}"""


def must_be_known(checker, ctx):
    return f"""not exists(
        select *
          from {checker.parameters.ref_entity} t
         where t.{checker.parameters.ref_attribute}::text = {ctx.entity}.{checker.parameters.attribute}
       )"""
