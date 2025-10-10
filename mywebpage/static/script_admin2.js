const formOpenBtn = document.querySelector("#form-open"),
    logoutButton=document.querySelector("#logout")
    home = document.querySelector(".home"),
    formContainer = document.querySelector(".form_container"),
    formCloseBtn = document.querySelector(".form_close"),
    signupBtn = document.querySelector("#signup"),
    loginBtn = document.querySelector("#login"),
    pwShowHide = document.querySelectorAll(".pw_hide");
    dropdownContent = document.getElementById('dropdownContent');
    buttonSubscription = document.getElementById('buttonSubscription');
    navItem = document.getElementById('navItem');

    
   
    const header = document.querySelector('.header0');
    


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



// Function to handle form submission
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
    .then(response => {
        // Check if the response indicates success
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }

        // Assuming the response is in a suitable format for Chart.js
        // You can directly use it for rendering charts on charts.html
        return response.text(); // Return the response text
    })
    .then(data => {
        // Log the data received for debugging
        console.log(data);

        // Assuming data is suitable for Chart.js consumption
        // You can use this data to render charts on charts.html
        // For example, you can pass this data to your Chart.js initialization code
        // or any other JavaScript code that handles chart rendering
    })
    .catch(error => {
        console.error('Error:', error);
    });
}




// Add an event listener for the "Submit" button
const submitButton = document.getElementById('submitButton');
submitButton.addEventListener('click', function (event) {
    event.preventDefault(); // Prevent the default form submission
    handleFormSubmission("submitForm", "/topicMonitoring");
});



logoutButton.addEventListener("click", function() {
    // Navigate to the "/logout" route
    window.location.href = "/logout";
});

// Function to change header background on scroll
function handleScroll() {
    if (window.scrollY > 0) {
    header.style.backgroundColor = 'rgba(0, 0, 0, 1)';
    } else {
    header.style.backgroundColor = 'rgba(0, 0, 0, 0.3)';
    }
}


document.getElementById('chatIframe').addEventListener('click', function() {
    // Load and display the third-party chat content
    loadThirdPartyChat();
  });

  


  

// Add event listener for scroll
window.addEventListener('scroll', handleScroll);

