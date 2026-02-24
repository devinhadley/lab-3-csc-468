{
  "op": "Project",
  "attrs": ["Enroll.sid", "Enroll.course"],
  "child": {
    "op": "Join",
    "condition": ["Student.sid", "=", "Enroll.sid"],
    "left": {
      "op": "Project",
      "attrs": ["Student.sid"],
      "child": {
        "op": "Select",
        "predicate": ["Student.major", "=", "CS"],
        "child": { "op": "Scan", "relation": "Student" }
      }
    },
    "right": {
      "op": "Project",
      "attrs": ["Enroll.sid", "Enroll.course"],
      "child": { "op": "Scan", "relation": "Enroll" }
    }
  }
}
