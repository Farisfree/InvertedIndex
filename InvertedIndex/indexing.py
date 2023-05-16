file_path = "C:/Users/a/Desktop/workshop2/project/1.txt"  # 替换为实际文件路径
with open(file_path, "r") as file:
    content = file.read()

lower_content = content.lower()  # 将单词全部变成小写
with open(file_path, "w") as file:
    file.write(lower_content)

words_to_delete = ["are"]  # 替换为要删除的单词列表


def remove_words(lower_content, words_to_delete):
    modified_content = lower_content
    for word in words_to_delete:
        modified_content = modified_content.replace(word, "")
    return modified_content


content_final = remove_words(lower_content, words_to_delete)
# 将转换后的内容写回文件
with open(file_path, "w") as file:
    file.write(str(content_final))
