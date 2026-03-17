import pandas as pd

# --------------------------
# Create the DataFrames
# --------------------------

students_data = {
    'student_id': [101, 102, 103, 104, 105, 106, 107],
    'name': ['Alice', 'Bob', None, 'David', 'Emma', 'Frank', 'Grace'],
    'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com',
              None, 'emma@email.com', 'frank@email.com', 'grace@email.com'],
    'city': ['Mumbai', 'Delhi', 'Bangalore', 'Mumbai', None, 'Chennai', 'Delhi']
}

enrollments_data = {
    'student_id': [101, 102, 103, 105, 108, 109],
    'course_name': ['Python', 'Data Science', 'Python', 'Machine Learning', 'AI', 'Python'],
    'enrollment_date': ['2024-01-15', '2024-01-20', '2024-02-01',
                        '2024-02-10', '2024-02-15', '2024-03-01']
}

scores_data = {
    'student_id': [101, 102, 104, 105, 106],
    'exam_score': [85, 92, 78, 88, 95]
}

# Convert to DataFrames
students_df = pd.DataFrame(students_data)
enrollments_df = pd.DataFrame(enrollments_data)
scores_df = pd.DataFrame(scores_data)

# --------------------------
# Missing Value Analysis
# --------------------------

print("Null Count:\n", students_df.isnull().sum())
print("\nNull Percentage:\n", (students_df.isnull().mean() * 100).round(2))

# --------------------------
# Clean the Students DataFrame
# --------------------------

# Fill missing city
students_df['city'] = students_df['city'].fillna('Unknown')

# Drop rows where name is missing
cleaned_students_df = students_df.dropna(subset=['name'])

print("\nCleaned Students DataFrame:\n", cleaned_students_df)

# --------------------------
# 1. INNER JOIN
# --------------------------

inner_join = pd.merge(students_df, enrollments_df, on='student_id', how='inner')
print("\n=== INNER JOIN RESULT ===\n", inner_join)

print("\nNumber of students in inner join:", len(inner_join))

excluded_students = set(students_df['student_id']) - set(inner_join['student_id'])
print("\nStudents excluded from inner join (no enrollment record):", excluded_students)

# --------------------------
# 2. LEFT JOIN
# --------------------------

left_join = pd.merge(students_df, enrollments_df, on='student_id', how='left')
print("\n=== LEFT JOIN RESULT ===\n", left_join)

print("\nTotal rows in left join:", len(left_join))

null_course_students = left_join[left_join['course_name'].isnull()][['student_id', 'name']]
print("\nStudents with NULL course_name (no enrollment record):\n", null_course_students)

# --------------------------
# 3. RIGHT JOIN
# --------------------------

right_join = pd.merge(students_df, enrollments_df, on='student_id', how='right')
print("\n=== RIGHT JOIN RESULT ===\n", right_join)

print("\nTotal rows in right join:", len(right_join))

missing_names = right_join[right_join['name'].isnull()][['student_id', 'course_name']]
print("\nstudent_ids present without student names (not found in students table):\n", missing_names)

# --------------------------
# 4. FULL OUTER JOIN
# --------------------------

outer_join = pd.merge(students_df, enrollments_df,
                      on='student_id', how='outer')
print("\n=== FULL OUTER JOIN RESULT ===\n", outer_join)

print("\nTotal rows in full outer join:", len(outer_join))

missing_info = outer_join[(outer_join['name'].isnull()) | 
                          (outer_join['course_name'].isnull())]
print("\nRows where name is NULL OR course_name is NULL:\n", missing_info)

# --------------------------
# 5. OUTER JOIN WITH INDICATOR
# --------------------------

outer_indicator = pd.merge(students_df, enrollments_df,
                           on='student_id', how='outer', indicator=True)

print("\n=== FULL OUTER JOIN WITH INDICATOR ===\n", outer_indicator)

print("\nDistribution of _merge column:\n")
print(outer_indicator['_merge'].value_counts())

import pandas as pd

# --------------------------
# Create the DataFrames
# --------------------------

students_data = {
    'student_id': [101, 102, 103, 104, 105, 106, 107],
    'name': ['Alice', 'Bob', None, 'David', 'Emma', 'Frank', 'Grace'],
    'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com',
              None, 'emma@email.com', 'frank@email.com', 'grace@email.com'],
    'city': ['Mumbai', 'Delhi', 'Bangalore', 'Mumbai', None, 'Chennai', 'Delhi']
}

scores_data = {
    'student_id': [101, 102, 104, 105, 106],
    'exam_score': [85, 92, 78, 88, 95]
}

# Clean student data (same as Task-1)
students_df = pd.DataFrame(students_data)
students_df['city'] = students_df['city'].fillna('Unknown')
students_df = students_df.dropna(subset=['name'])

scores_df = pd.DataFrame(scores_data)

# ---------------------------------------
# 1. LOOKUP OPERATION USING .map()
# ---------------------------------------

# Create dictionary for lookup
score_dict = dict(zip(scores_df['student_id'], scores_df['exam_score']))
print("\nScore Dictionary:\n", score_dict)

# Add exam score to students_df using map
students_df['exam_score_map'] = students_df['student_id'].map(score_dict)

print("\nStudents with Scores (Using map):\n", students_df)

# ---------------------------------------
# 2. PERFORMANCE COMPARISON USING MERGE
# ---------------------------------------

merged_scores = students_df.merge(scores_df, on='student_id', how='left')

print("\nStudents with Scores (Using merge):\n", merged_scores)

# Explanation:
explanation = """
Why map() is faster than merge() for lookup:
- map() performs a simple dictionary lookup → O(1) time for each row
- merge() needs to:
    • align rows
    • sort/join tables
    • manage extra columns
  This adds overhead, making merge slower for single-column lookups.
"""
print(explanation)

# ---------------------------------------
# 3. AUTOMATION FUNCTION FOR MERGING
# ---------------------------------------

def auto_merge(df1, df2, join_type, key_column):
    """
    Automatically merges two DataFrames and returns details.
    """
    result = pd.merge(df1, df2, on=key_column, how=join_type)
    row_count = len(result)

    return {
        'result_df': result,
        'row_count': row_count,
        'join_type': join_type
    }

# Test the function with two join types
test1 = auto_merge(students_df, scores_df, 'left', 'student_id')
test2 = auto_merge(students_df, scores_df, 'inner', 'student_id')

print("\n=== AUTO MERGE TEST 1 (LEFT JOIN) ===")
print(test1['result_df'])
print("Row Count:", test1['row_count'])
print("Join Type:", test1['join_type'])

print("\n=== AUTO MERGE TEST 2 (INNER JOIN) ===")
print(test2['result_df'])
print("Row Count:", test2['row_count'])
print("Join Type:", test2['join_type'])

