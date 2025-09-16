fetch("/students/api")
  .then(response => response.json())
  .then(data => {
    console.log(data); // list of students from backend
  });
