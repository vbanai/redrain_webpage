

const formOpenBtn=document.querySelector("#form-open"),
  home=document.querySelector(".home"),
  formContainer=document.querySelector(".form_container"),
  formCloseBtn=document.querySelector(".form_close"),
  signupBtn=document.querySelector("#signup"),
  loginBtn=document.querySelector("#login"),
  pwShowHide=document.querySelectorAll(".pw_hide"),
  flashMessagesContainer = document.getElementById("flash-messages-container");
  resetRequestLink = document.getElementById("forgot_pw")
  
  const header = document.querySelector('.header0');
  let slogan = document.querySelector(".slogan"); // Define slogan variable globally

 

  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
      e.preventDefault();
      document.querySelector(this.getAttribute('href')).scrollIntoView({
        behavior: 'smooth',
        block: 'start'
      });
    });
  });

// Function to change header background on scroll
function handleScroll() {
  if (window.scrollY > 0) {
    header.style.backgroundColor = 'rgba(0, 0, 0, 1)';
  } else {
    header.style.backgroundColor = 'rgba(0, 0, 0, 0.3)';
  }
}
// Function to adjust slogan position on window resize
function adjustSloganPosition() {
  const homeHeight = home.clientHeight;
  const topPosition = (76 / 100) * homeHeight; // Calculate 80% of the height of the .home element
  slogan.style.top = topPosition + "px"; // Set the top position of the slogan
}

// Add scroll event listener
window.addEventListener('scroll', handleScroll);

window.addEventListener('resize', adjustSloganPosition);

// Initial adjustment of slogan position
adjustSloganPosition();
//---------------------------------------------- FLASH ------------------------------------------
  // Function to add a flash message to the fixed element
function addFlashMessage(message, category) {
  const flashMessagesContainer = document.getElementById("flash-messages");

  // Create a new div for the flash message
  const flashMessage = document.createElement("div");
  flashMessage.className = `flash-message ${category}`;
  flashMessage.textContent = message;

  // Append the flash message to the container
  flashMessagesContainer.appendChild(flashMessage);

  // Automatically remove the flash message after a certain time (e.g., 5 seconds)
  setTimeout(() => {
      flashMessage.remove();
  }, 5000);
}

  // Function to remove all flash messages
  function removeFlashMessages() {
    while (flashMessagesContainer.firstChild) {
      flashMessagesContainer.removeChild(flashMessagesContainer.firstChild);
    }
  }

 
    formOpenBtn.addEventListener("click", () => {
      home.classList.add("show");
      slogan.classList.add("dark");
      removeFlashMessages(); // Remove flash messages when opening the form
    });
    
    // formCloseBtn.addEventListener("click", () => {
    //   home.classList.remove("show")
    //   slogan.classList.remove("dark");
    // })
   

//---------------------------------------------- FLASH END ------------------------------------------

  // pwShowHide.forEach((icon) =>{
  //   icon.addEventListener("click", ()=>{
  //     let getPwInput=icon.parentElement.querySelector("input");
  //     if(getPwInput.type==="password"){
  //       getPwInput.type="text";
  //       icon.classList.replace("uil-eye-slash", "uil-eye");
  //     }else{
  //       getPwInput.type="password";
  //       icon.classList.replace("uil-eye", "uil-eye-slash");
  //     }
  //   });
 
  // });

  // signupBtn.addEventListener("click", (e) => {
  //   e.preventDefault();
  //   formContainer.classList.add("active");
  // });

  // loginBtn.addEventListener("click", (e) => {
  //   e.preventDefault();
  //   formContainer.classList.remove("active");
  // })



//-----------------------------  FORMSUBMISSION --------------------

//   function handleFormSubmission(formId, endpoint) {
//     const form = document.getElementById(formId);

//     form.addEventListener("submit", function (event) {
//         event.preventDefault();
//         console.log("Form submitted");

//         const emailField = form.querySelector("input[name='email']");
//         const passwordField = form.querySelector("input[name='password']");

//         if (!emailField || !passwordField) {
//             console.error("Email or password field not found in the form");
//             return;
//         }

//         const email = emailField.value;
//         const password = passwordField.value;

//         console.log('Email:', email);
//         console.log('Password:', password);

//         const formData = new FormData(form);
//         formData.append('email', email);
//         formData.append('password', password);

//         // Check if confirm_password field exists (for signup form)
//         const confirm_password_field = form.querySelector("input[name='confirm_password']");
//         if (confirm_password_field && form.classList.contains('registration-form')) {
//             const confirm_password = confirm_password_field.value;
//             console.log('Confirm Password:', confirm_password);
//             formData.append('confirm_password', confirm_password);
//         }

//         console.log(formData);

//         fetch(endpoint, {
//           method: 'POST',
//           body: formData
//         }).then(response => {
//           if (response.ok) {
//             console.log('Form submitted successfully');
//             return response.json();
//           } else {
//             console.error('Form submission failed');
//             return response.json();
//           }
//         }).then(data => {
//           console.log('Server response:', data);

          
//         }).catch(error => {
//           console.error('Fetch error:', error);
//         });
//       });
// }

// // Add an event listener for the "Login Now" button
// const loginButton = document.getElementById('loginButton');
// loginButton.addEventListener('click', async function (event) {
//     event.preventDefault(); // Prevent the default form submission behavior

//     // Call your existing form submission logic
//     const loginSuccess = await handleFormSubmission("loginForm", "/login");
//     console.log('Login success:', loginSuccess);
//     if (loginSuccess) {
//         // Redirect to the desired URL after successful login
//         window.location.href = '/serviceselector'; // Replace with your desired URL
//     }else{
//       window.location.href = '/';
//     }
    
// });

// const signupButton = document.getElementById('signupButton');

// signupButton.addEventListener('click', async function (event) {
//     event.preventDefault(); // Prevent the default form submission behavior

//     // Call your existing form submission logic
//     const signupSuccess = await handleFormSubmission("signupForm", "/signup"); // Use the correct form ID and endpoint
//     console.log('Signup success:', signupSuccess);
//     if (signupSuccess) {
//         // Redirect to the desired URL after successful signup
//         window.location.href = '/welcome'; // Replace with your desired URL
//     } else {
//         window.location.href = '/'; // Redirect to home or an error page on failure
//     }
// });

// Function to handle form submission asynchronously
// async function handleFormSubmission(formId, endpoint) {
//     const formData = new FormData(document.getElementById(formId));

//     try {
//         // Use fetch or your preferred method for submitting the form data to the server
//         const response = await fetch(endpoint, {
//             method: 'POST',
//             body: formData
//             // Add other necessary options (headers, etc.)
//         });

//         // Check if the server response indicates success
//         if (response.ok) {
//             // You can perform additional checks based on the server response if needed
//             console.log('Form submitted successfully');
//             return true;
//         } else {
//             console.error('Form submission failed');
//             return false;
//         }
//     } catch (error) {
//         console.error('An error occurred during form submission:', error);
//         return false;
//     }
// }


// // Add an event listener for the "Signup Now" button
// const signupButton = document.getElementById('signupButton');
// signupButton.addEventListener('click', function () {
//     handleFormSubmission("signupForm", "/signup");
// });

// function fitTextToContainer() {
//   const elements = document.querySelectorAll('.extra-text-chat, .extra-text-chat-maintenance');

//   // Set an initial large font size for testing and reset all elements
//   let minOverallFontSize = 100; 

//   // Step 1: Find the smallest fitting font size across all elements
//   elements.forEach(element => {
//     const textElement = element.querySelector('.smartchat_text');
    
//     // Ensure textElement exists
//     if (!textElement) return;

//     let minFontSize = 8; // Minimum font size to prevent text from getting too small
//     let maxFontSize = 100; // Initial maximum font size for testing

//     while (minFontSize <= maxFontSize) {
//       const midFontSize = Math.floor((minFontSize + maxFontSize) / 2);
//       textElement.style.fontSize = midFontSize + 'px';

//       const fitsInHeight = textElement.scrollHeight <= element.clientHeight;
//       const fitsInWidth = textElement.scrollWidth <= element.clientWidth;

//       if (fitsInHeight && fitsInWidth) {
//         // Text fits within the container, try increasing the font size
//         minFontSize = midFontSize + 1;
//       } else {
//         // Text overflows, decrease the font size
//         maxFontSize = midFontSize - 1;
//       }
//     }

//     // After binary search, maxFontSize is the largest that fits, so check against minOverallFontSize
//     minOverallFontSize = Math.min(minOverallFontSize, maxFontSize);
//   });

//   // Step 2: Apply the smallest fitting font size to all elements
//   elements.forEach(element => {
//     const textElement = element.querySelector('.smartchat_text');
    
//     // Ensure textElement exists
//     if (textElement) {
//       textElement.style.fontSize = minOverallFontSize + 'px';
//     }
//   });
// }

// window.addEventListener('resize', fitTextToContainer);
// window.addEventListener('load', fitTextToContainer);






// document.getElementById('showChatButton').addEventListener('click', function() {
//   // Load and display the third-party chat content
//   loadThirdPartyChat();
// });







