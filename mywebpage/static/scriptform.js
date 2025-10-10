const formOpenBtn = document.querySelector("#form-open"),
    logoutButton=document.querySelector("#logout")
    home = document.querySelector(".home"),
    formContainer = document.querySelector(".form_container"),
    formCloseBtn = document.querySelector(".form_close"),
    signupBtn = document.querySelector("#signup"),
    loginBtn = document.querySelector("#login"),
    pwShowHide = document.querySelectorAll(".pw_hide");

formOpenBtn.addEventListener("click", () => home.classList.add("show"));
formCloseBtn.addEventListener("click", () => home.classList.remove("show"));

pwShowHide.forEach((icon) => {
    icon.addEventListener("click", () => {
        let getPwInput = icon.parentElement.querySelector("input");
        if (getPwInput.type === "password") {
            getPwInput.type = "text";
            icon.classList.replace("uil-eye-slash", "uil-eye");
        } else {
            getPwInput.type = "password";
            icon.classList.replace("uil-eye", "uil-eye-slash");
        }
    });
});

function handleFormSubmission(formId, endpoint) {
  const form = document.getElementById(formId);
  const formData = new FormData(form);

  // Get the CSRF token from the meta tag
  const csrfTokenElement = document.querySelector('meta[name=csrf-token]');
  
  if (!csrfTokenElement) {
      console.error('CSRF token meta tag not found.');
      return;
  }

  const csrfToken = csrfTokenElement.content;

  // Include CSRF token in the headers
  const headers = new Headers({
      'X-CSRFToken': csrfToken
  });

  // Fetch API for making the POST request
  fetch(endpoint, {
      method: 'POST',
      body: formData,
      headers: headers
  })
      .then(response => response.json())
      .then(data => {
          console.log(data); // Handle the response as needed
      })
      .catch(error => {
          console.error('Error:', error);
      });
}


// Add an event listener for the "Submit" button
const submitButton = document.getElementById('submitButton');
submitButton.addEventListener('click', function (event) {
    event.preventDefault(); // Prevent the default form submission
    handleFormSubmission("submitForm", "/submitForm");
});



logoutButton.addEventListener("click", function() {
    // Navigate to the "/logout" route
    window.location.href = "/logout";
});

