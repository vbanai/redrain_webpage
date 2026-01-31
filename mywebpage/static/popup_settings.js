             // Storing the user id to track if this user is recurring





// ----------------------------------------------
// First act: we have the loader file on azure blob, and when host page is loading it 
// gets the loader as its own js. It will run it, loader check userid is stored if not create one
// and send its Post request to the backend route "/" where in the url request it includes client id, user id apikey 
// if user id is included it is fine backend doesn't create newone, if post validated the loader
// goes into the get request process send another request wiwth the  url again, same on backend first
// check the userid if not create one, and finally serve the html pass the variables and run the popup js as well
// and we reach the socket creation part where we send the auth params to the Socket.IO backend. 
//Is it a Socket.IO/WebSocket request? → handled by `sio`. Otherwise → passed to `fastapi_app` (HTTP routes) 



      // CREATING and ADMINISTRATE A WEBSOCKET CONNECTION TO THE BACKEND





let botMode=client_initial_mode
let user_id_forMode=userId
let client_id_forMode=client_code
let overallMode=client_initial_mode



// Send user message to the server via Socket.IO
// const urlParams = new URLSearchParams(window.location.search);
// const client_code = urlParams.get('client_code');
// const api_key = urlParams.get('api_key');


document.addEventListener("DOMContentLoaded", function() {

            // Get URL parameters  
        
  // Loader sets the iframe URL after POST validation: iframe.src = `${backend_url}/?client_code=${encodeURIComponent(client_code)}&iframeWidth=${iframeWidth}&chatBodyHeight=${chatBodyHeight}&user_id=${encodeURIComponent(userId)}`;
  // This triggers a GET request from the browser to the backend, because the iframe is now pointing to that URL.
  // Backend GET route
  // Backend receives the GET request.
  // Reads query parameters from URL (client_code, user_id, iframeWidth, chatBodyHeight).
  // Generates the popup HTML (softcodedPOPUP.html) and injects template variables, including user_id.
  // Sends this HTML back to the iframe.
  // The browser loads the iframe with that URL: http://backend-url/?client_code=abc123&iframeWidth=420&chatBodyHeight=300&user_id=xyz
  // Inside the iframe, popup.js runs, and window.location.search contains: "?client_code=abc123&iframeWidth=420&chatBodyHeight=300&user_id=xyz"


const urlParams = new URLSearchParams(window.location.search);
let width = urlParams.get('iframeWidth') || 420; // Default to 420px

// Select elements
const chatbotPopup = document.querySelector('.chatbot-popup');
const chatBody = document.querySelector('.chat-body');



// Listen for size updates from the parent window  //közvetlenül a browsertől(iframet tartalmazó) kapjuk az adatokat
  // Listen for size updates from the parent window  //közvetlenül a browsertől(iframet tartalmazó) kapjuk az adatokat
  // As I have in loader:   if (iframe && iframe.contentWindow) { this references the window object of the iframe.
                  // iframe.contentWindow.postMessage( this sends a message from the parent to that iframe.
                  //     { iframeWidth, chatBodyHeight }, The object { iframeWidth, chatBodyHeight } is the payload.
                  //     "*"

});
//requestAnimationFrame make sure smooth transition when parameters changes



const chatBody=document.querySelector(".chat-body")
const messageInput=document.querySelector(".message-input")
const sendMessageButton=document.querySelector("#send-message")
const fileInput=document.querySelector("#file-input")
const pastePreview = document.querySelector(".paste-preview");
const fileUploadWrapper=document.querySelector(".file-upload-wrapper");
const fileCancelButton=document.querySelector("#file-cancel");

const closeChatbot=document.querySelector("#close-chatbot");



function showTempMessage(text) {
  const footer = document.querySelector(".chat-footer");
  if (!footer) return;

  // Remove any existing temp message first
  const existing = footer.querySelector(".temp-message");
  if (existing) existing.remove();

  // Create new message
  const tempMsg = document.createElement("div");
  tempMsg.className = "temp-message";
  tempMsg.textContent = text;

  footer.appendChild(tempMsg);

  // Auto-remove after 4 seconds
  setTimeout(() => tempMsg.remove(), 4000);
}






const userData={
message:null,
file:{
data:null,
mime_type: null
}
}


const initalInputHeight=messageInput.scrollHeight;


// Create message element with dynamic classes and return it
const createMessageElement = (content, ...classes) => {
const div=document.createElement("div");
div.classList.add("message", ...classes);
div.innerHTML=content;
return div;
}



const handleOutgoingMessage=(e)=>{

e.preventDefault();
userData.message=messageInput.value.trim()


if (!userData.message) return;
// Send the message to the backend



messageInput.value="";
fileUploadWrapper.classList.remove("file-uploaded")
messageInput.dispatchEvent(new Event("input"));





//////////////////////////////////
//create and display user message
//////////////////////////////////





const messageContent = `<div class="message-text"></div>
${userData.file.data ? `<img src="${userData.file.data}" class="attachment" />` : ""}`;


const outgoingMessageDiv= createMessageElement(messageContent, "user-message");
outgoingMessageDiv.querySelector(".message-text").textContent=userData.message;
chatBody.appendChild(outgoingMessageDiv);
chatBody.scrollTo({top: chatBody.scrollHeight, behavior: "smooth"});

let botmessageContent;
console.log(user_id_forMode)
console.log(userId)
console.log(client_id_forMode)
console.log("BOTMODE ", botMode)
console.log("Client Code: ", client_code)

if  (botMode == 'automatic' || overallMode == 'automatic'){
// <svg class="bot-avatar" xmlns="http://www.w3.org/2000/svg" width="50" height="50" 
// viewBox="0 0 1024 1024">
//   <path d="M738.3 287.6H285.7c-59 0-106.8 47.8-106.8 106.8v303.1c0 59 47.8 106.8 106.8 106.8h81.5v111.1c0 .7.8 1.1 1.4.7l166.9-110.6 41.8-.8h117.4l43.6-.4c59 0 106.8-47.8 106.8-106.8V394.5c0-59-47.8-106.9-106.8-106.9zM351.7 448.2c0-29.5 23.9-53.5 53.5-53.5s53.5 23.9 53.5 53.5-23.9 53.5-53.5 53.5-53.5-23.9-53.5-53.5zm157.9 267.1c-67.8 0-123.8-47.5-132.3-109h264.6c-8.6 61.5-64.5 109-132.3 109zm110-213.7c-29.5 0-53.5-23.9-53.5-53.5s23.9-53.5 53.5-53.5 53.5 23.9 53.5 53.5-23.9 53.5-53.5 53.5zM867.2 644.5V453.1h26.5c19.4 0 35.1 15.7 35.1 35.1v121.1c0 19.4-15.7 35.1-35.1 35.1h-26.5zM95.2 609.4V488.2c0-19.4 15.7-35.1 35.1-35.1h26.5v191.3h-26.5c-19.4 0-35.1-15.7-35.1-35.1zM561.5 149.6c0 23.4-15.6 43.3-36.9 49.7v44.9h-30v-44.9c-21.4-6.5-36.9-26.3-36.9-49.7 0-28.6 23.3-51.9 51.9-51.9s51.9 23.3 51.9 51.9z" />
//     </svg>
botmessageContent=`
<div class="bot-avatar" style="color: white">
<span class="material-symbols-outlined graph-icon">graph_6</span>
</div>
  <div class="message-text">
    <div class="thinking-indicator">
      <div class="dot"></div>
      <div class="dot"></div>
      <div class="dot"></div>
    </div>
  </div>`;

}
// if (user_id_forMode === userId && client_id_forMode === client_code && (botMode == 'manual' || overallMode == 'manual')){
  if (botMode == 'manual' || overallMode == 'manual'){


    const existingTyping = chatBody.querySelector(
    `.message.bot-message.thinking[data-user-id="${userId}"]`
  );
  
  if (existingTyping) {
    existingTyping.remove();
  }

botmessageContent=`
<div class="human-avatar">
  <span class="material-symbols-outlined" style="font-size: 32px; color: white;">
    support_agent
  </span>
</div>



<div class="manual-typing-indicator" style="font-style: italic; font-size: 12.5px">Sales colleague is typing </div>
<div class="message-text">
    <div class="thinking-indicator">
      <div class="dot"></div>
      <div class="dot"></div>
      <div class="dot"></div>
    </div>
  </div>`;
}
// Create a placeholder for bot response (typing indicator)


const incomingMessageDiv= createMessageElement(botmessageContent, "bot-message", "thinking");
incomingMessageDiv.dataset.userId = userId;

chatBody.appendChild(incomingMessageDiv);
chatBody.scrollTo({ top: chatBody.scrollHeight, behavior: "smooth" });
// Store the bot message div reference so it can be updated later




}





messageInput.addEventListener("keydown", (e)=>{
const userMessage=e.target.value.trim();
if(e.key==="Enter" && userMessage && !e.shiftKey && window.innerWidth>768){
handleOutgoingMessage(e);
}
});



messageInput.addEventListener("input", () =>{
messageInput.style.height=`${initalInputHeight}px`;
messageInput.style.height=`${messageInput.scrollHeight}px`;
document.querySelector(".chat-form").style.borderRadius=messageInput.scrollHeight>initalInputHeight ? "15px":"32px";
})


const picker = new EmojiMart.Picker({
theme: "light",
skinTonePosition:"none",
previewPosition: "none",
onEmojiSelect:(emoji)=>{
const {selectionStart: start, selectionEnd: end}=messageInput;
messageInput.setRangeText(emoji.native, start, end, "end")
messageInput.focus();
},
onClickOutside:(e) => {
if(e.target.id==="emoji-picker"){
document.body.classList.toggle("show-emoji-picker");
}else{
document.body.classList.remove("show-emoji-picker");
}
}

})


// Shared handler function (language aware)
    function handleAgentRequest(userMessage, botMessage) {

      // Lock input if automatic mode
      if (botMode === 'automatic' || overallMode === 'automatic') {
        lockInput();
      }
      const confirmPanel = document.getElementById("human-confirm");
      confirmPanel.classList.add("hidden");

      // --- 1. Display user message ---
      const userMsgDiv = createMessageElement(`<div class="message-text"></div>`, "user-message");
      userMsgDiv.querySelector(".message-text").textContent = userMessage;
      chatBody.appendChild(userMsgDiv);
      chatBody.scrollTo({ top: chatBody.scrollHeight, behavior: "smooth" });

      // --- 2. Display bot “connecting” message ---
      const botMsgHTML = `
        <div class="bot-avatar" style="color: white;">
          <span class="material-symbols-outlined graph-icon">graph_6</span>
        </div>
        <div class="message-text">
          ${botMessage}
          <div class="thinking-indicator" style="margin-top: 16px;">
            <div class="dot"></div>
            <div class="dot"></div>
            <div class="dot"></div>
          </div>
        </div>
      `;

      const botThinkingDiv = createMessageElement(botMsgHTML, "bot-message", "thinking");
      botThinkingDiv.dataset.userId = userId;
      chatBody.appendChild(botThinkingDiv);
      chatBody.scrollTo({ top: chatBody.scrollHeight, behavior: "smooth" });
      
      // Keep reference so we can later remove it
      window.lastThinkingDiv = botThinkingDiv;

  
    }





document.querySelector(".chat-form").appendChild(picker)
sendMessageButton.addEventListener("click", (e) => handleOutgoingMessage(e))



const fileUploadBtn = document.querySelector("#file-upload");

const wrapper = document.querySelector(".file-upload-wrapper");

// Update class based on mode
function updateUploadMode() {
  if (botMode === 'automatic' || overallMode === 'automatic') {
    wrapper.classList.add("automatic");
  } else {
    wrapper.classList.remove("automatic");
  }
}

fileUploadBtn.addEventListener("click", () => {
  if (botMode === 'automatic' || overallMode === 'automatic') {
    // Prevent file picker in automatic mode
    return;
  }

  fileInput.click();
});

// Call once when mode changes
updateUploadMode();



document.addEventListener("DOMContentLoaded", () => {
  document.body.classList.add("show-chatbot");
  const chatbotToggler = document.querySelector("#chatbot-toggler");
  const closeChatbot = document.querySelector("#close-chatbot");

  if (chatbotToggler) {
    chatbotToggler.addEventListener("click", () => {
      const isOpen = document.body.classList.toggle("show-chatbot");
    
      // .toggle() does two things at the same time: It adds OR removes the class And it RETURNS a value (true or false)
      const message = { chatbotOpen: isOpen };
    
      // Dynamically get the parent origin
      const parentOrigin = window.location !== window.parent.location 
        ? document.referrer 
        : window.location.origin;

      window.parent.postMessage(message, parentOrigin);
    });
  }

    const askHumanBtn = document.getElementById("ask-human");
    const confirmPanel = document.getElementById("human-confirm");
   
    const closeConfirm = document.getElementById("close-confirm");

    // Only one language: just show first message immediately
    const firstMessage = document.getElementById("first-bot-message");
    const messageText = firstMessage.querySelector(".message-text");
    firstMessage.style.display = "flex";

    const lang = selectedLanguage;
    

    updateUIText(lang);
    firstMessage.scrollIntoView({ behavior: "smooth" });

    // Toggle visibility of Ask Human icon
    messageInput.addEventListener("input", () => {
      askHumanBtn.style.display = messageInput.value.trim().length > 0 ? "none" : "inline-block";
    });

    // Show confirmation popup when clicked
    askHumanBtn.addEventListener("click", () => {
      confirmPanel.classList.toggle("hidden");
    });

    
   

    // Close confirmation panel
    closeConfirm.addEventListener("click", () => {
      confirmPanel.classList.add("hidden");
    });

    // Hide panel when clicking outside it

   


  if (closeChatbot) {
    closeChatbot.addEventListener("click", () => {
      document.body.classList.remove("show-chatbot");
      const message = { chatbotOpen: false };
     
      const parentOrigin = window.location !== window.parent.location 
      ? document.referrer 
      : window.location.origin;

      window.parent.postMessage(message, parentOrigin);
    });
  }
});




const selectHU = document.getElementById("select-hu");
const selectEN = document.getElementById("select-en");
const languageSelector = document.getElementById("language-selector");
const chatMessages = document.getElementById("chat-messages");


let selectedLanguage = supportedLanguages[0];








function updateUIText(lang) {
  const t = translations[lang];

  document.getElementById("human-confirm-text").textContent = t.humanConfirmText;
  document.querySelector(".confirm-label").textContent = t.confirmYes;

  document.querySelector(".logo-text").textContent = t.logo_text;
  document.querySelector(".message-input").placeholder = t.sendPlaceholder;
}


// function showHumanConfirm(lang) {
//   const popup = document.getElementById("human-confirm");
//   const textEl = document.getElementById("human-confirm-text");
//   const btnHU = document.getElementById("confirm-yes-hu");
//   const btnEN = document.getElementById("confirm-yes-en");

//   // show popup
//   popup.classList.remove("hidden");

//   if (lang === "hu") {
//     textEl.textContent = "Szeretne ügyintézővel beszélni?"; //always appears on the left, because your CSS makes the popup display: flex
//     btnHU.style.display = "inline-flex";   // show Hungarian button
//     btnEN.style.display = "none";          // hide English button
//   } else {
//     textEl.textContent = "Would you like to talk to a colleague?";
//     btnHU.style.display = "none";          // hide Hungarian button
//     btnEN.style.display = "inline-flex";   // show English button
//   }
// }































// ====================================================================
// RUN EVERYTHING ON LOAD
// ====================================================================
window.addEventListener("DOMContentLoaded", () => {


     
  

  

  let idleTimeout = 60 * 60 * 1000; // 1 minutes
  let lastActivity = Date.now();

  function resetActivity() {
      lastActivity = Date.now();
  }

  // Capture activity
  document.addEventListener('mousemove', resetActivity);
  document.addEventListener('keydown', resetActivity);
  document.addEventListener('scroll', resetActivity);

  // Heartbeat function
  async function sendHeartbeat() {
    const now = Date.now();
    if (now - lastActivity < idleTimeout) {
        try {
            const resp = await fetch('/heartbeat', { method: 'POST', credentials: 'include' });
            //Tells the browser to include cookies from the current site in the request, even for cross-origin requests.
            // Body: You don’t send a body here, so nothing is explicitly sent in the request payload.
            console.log("[Heartbeat] sent", resp.status);
        } catch (e) {
            console.error("[Heartbeat] failed", e);
        }
    } else {
        console.log("[Heartbeat] idle, not sending");
    }
}

  // Send heartbeat every 1 min
  setInterval(sendHeartbeat, 60 * 1000);





 

});

