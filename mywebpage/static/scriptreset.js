 // Add an event listener for the "Login Now" button

 const resetButton = document.getElementById('resetButton');

 resetButton.addEventListener('click', async function (event) {
     event.preventDefault(); // Prevent the default form submission behavior

     // Call your existing form submission logic
     const resetsubmitSuccess = await handleFormSubmission_reset("resetForm", "/reset_password");
     console.log('Reset submit success:', resetsubmitSuccess);
     if (resetsubmitSuccess) {
      // Redirect to the desired URL after successful login
         window.location.href = '/'; // Replace with your desired URL
     }else{
       window.location.href = '/reset_password';
     }
    
 });

 // Function to handle form submission asynchronously
 async function handleFormSubmission_reset(formId, endpoint) {
     const formData = new FormData(document.getElementById(formId));

     try {
         // Use fetch or your preferred method for submitting the form data to the server
         const response = await fetch(endpoint, {
             method: 'POST',
             body: formData
             // Add other necessary options (headers, etc.)
         });

         // Check if the server response indicates success
         if (response.ok) {
             // You can perform additional checks based on the server response if needed
             console.log('Form submitted successfully');
             return true;
         } else {
            console.error('Form submission failed');
             return false;
        }
    } catch (error) {
        console.error('An error occurred during form submission:', error);
         return false;
    }
}


