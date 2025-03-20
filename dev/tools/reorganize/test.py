# import re

# sql_line = "(12.00,'JAPAN',2,'ously. final, express gifts cajole a')"
# # 正则表达式模式，用于匹配括号内用逗号分隔的值
# pattern = r"\((.*)\)"
# match = re.search(pattern, sql_line)
# if match:
#     values_str = match.group(1)
#     # 正则表达式模式，用于匹配单引号引起来的值或数字
#     value_pattern = r"'[^']*'|\d+"
#     values = re.findall(value_pattern, values_str)
#     # 去除单引号
#     # result = [value.strip("'") for value in values]
#     print(values)

import re

line = "(2230,1,1,1664,'2024-10-23 17:03:47',NULL,15,1)"

# 正则表达式模式，用于匹配每一个列的值，添加了对 NULL 的匹配
matches = re.findall(r"'(.*?)'|(\d+\.\d+)|(\d+)|(NULL)", line)

# 处理匹配结果，将字符串加上引号，数字按格式转换，保留 NULL
parsed_line = [
    f"'{m[0]}'" if m[0] else m[1] if m[1] else m[2] if m[2] else m[3] for m in matches
]

print(parsed_line)