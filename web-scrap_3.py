from bs4 import BeautifulSoup
import requests
import csv


url = "https://stackpython.co/courses"

res = requests.get(url)
res.encoding = "utf-8"
print(res)

soup = BeautifulSoup(res.text, 'html.parser')
# print(soup)
# print(soup.prettify())

courses = soup.find_all('h2')
print(courses)

# Create an empty list
course_list = []

for course in courses:

    # Create new variable --> obj to store
    # only course name getting rid of unwanted tags
    obj = course.string

    # Append each course into a course_list variable
    course_list.append(obj)


print(course_list)

# Define row and column (DataFrame)
csv_col = [['title'], [course_list]]

# Name a file, and put w as an argument to tell this is "writer" file
f = open('filename.csv', 'w')
with f:
    writer = csv.writer(f)

    for row in csv_col:
        writer.writerow(row)

# Display type of this object (Of course, it's "list")
print(type(course_list))

# Test accessing position 1 of the list
print(course_list[1])

# To count how many courses in the list
print(len(course_list))
