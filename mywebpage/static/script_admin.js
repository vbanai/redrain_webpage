
const toggleButton = document.getElementById('toggle-response-mode');
const tabsInputContainer = document.getElementById('tabs-input-container');
const tabsContainer = document.getElementById('tabs');
const tabContentsContainer = document.getElementById('tab-contents');
let manualMode = false;
let isTabCreated=false;
let eventSource;
const colleaguesChats = {}; // To store chat boxes per tab
const rectangle = {};
const locations = {};
let activeRectangle = null;
const activeRectangles = {}; // To store the active rectangle for each tab
const isUserRectangleClickedPerTab = {}; // To track if a user rectangle is clicked in each tab
let isUserRectangleClicked = false;
const topMiddleButtons={};
const Chats_automatic = {}; // To store chat boxes per tab
const rectangle_automatic = {}; // To store user rectangles per tab
const locations_automatic = {}; // To store location boxes per tab
let messageCount = 0; // Counter to keep track of messages for round-robin distribution
const maxFontSize = 15; // Set your maximum font size here
const minFontSize = 8; // Optionally, set a minimum font size
const editTabsButton = document.getElementById('edit-tabs-button');
const editTabsContainer = document.getElementById('edit-tabs-container');
const backButton = document.getElementById('back-button');
const addColleagueButton = document.getElementById('add-colleague-button');
const removeColleagueButton = document.getElementById('remove-colleague-button');
const userElements = {};
const userButtons = {};
const userButtonStates={};
const automaticResponseStates = {};
const counterForAddAdminMessage={};
const counterForManualModeAddMessage={};
let prependeduserId=0
let currentTabMode = 'input';
let dotInterval_historyloading;
let clientTimezone = 'UTC';
let isBatchMode = false;  // global or scoped flag
let totalMessagesToSend = 0;  // total messages in current batch
let sentMessageCount = 0;  // messages sent so far in batch



const input = document.getElementById('colleagues');
const inputAddOneColleague = document.getElementById('add-colleague');
const removeOneColleague = document.getElementById('remove-colleague');
const dropdownAddOneColleague = document.getElementById('dropdown-add-colleague');
const dropdownRemoveOneColleague = document.getElementById('dropdown-remove-colleague');


// Add the keydown listener once on page load
document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape') {
    
      
      // Get the current active tab index
      const activeTab = document.querySelector('.tab.active');
      const activeTabIndex = activeTab ? activeTab.dataset.tabId : null;

      if (!activeTabIndex) {
          console.error('No active tab detected!');
          return;
      }

      // Find all selected rectangles only within the current tab
      const selectedRects = document.querySelectorAll(`.user-rectangle.ctrl-click-selected[data-tab-index="${activeTabIndex}"]`);
      selectedRects.forEach(rect => {
          rect.classList.remove('ctrl-click-selected', 'user-rectangle-hover-lightgreen', 'user-rectangle-hover-lightblue');

          if (rect.dataset.flag === 'true') {
              rect.classList.add('default-green');
          } else {
              rect.classList.add('default-blue');
          }
      });
  }
});


//                 FUNCTION TO CREATE DEFAULT TAB FOR AUTOMATIC MODE  


// 👇 Font size calculation function
function updateFontSize() {
  let newFontSize = window.innerWidth / 50;
  newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
  topLeftSection.style.fontSize = `${newFontSize}px`;
}

function createDefaultTabForAutomaticMode() {

  if (manualMode) return; // Do nothing if in manual mode

  // Clear previous tabs and contents
  tabsContainer.innerHTML = '';
  tabContentsContainer.innerHTML = '';

 
  // Create the default "Automatic Chatbot" tab
  const tab = document.createElement('div');
  const language = localStorage.getItem('language') || 'hu';
  // Set the tab text based on the selected language
  tab.textContent = translations[language]['automaticChatbot'] || 'Automatic Chatbot'; // Fallback to English if translation is not found

  // tab.textContent = 'Automatic Chatbot';
  tab.id = 'automatic-chatbot-tab'; 
  tab.classList.add('tab', 'active'); // Set it as active
  tabsContainer.appendChild(tab);

  // Create tab content
  const content = document.createElement('div');
  content.classList.add('tab-content', 'active'); // Set it as active
  tabContentsContainer.appendChild(content);


  // Create Grid Layout for Tab Content with 2 rows and 3 columns
  const topRow = document.createElement('div');
  topRow.classList.add('top-row');

  
  const topLeftSection = document.createElement('div');
  topLeftSection.classList.add('top-left-section');
  // topLeftSection.textContent = 'Customers';
  topLeftSection.textContent = translations[language]['customers'] || 'Ügyfelek';
  topLeftSection.style.fontWeight = 'bold';
  
  // Calculate the font size based on the window width
//   window.addEventListener('resize', () => {
//     // Calculate new font size based on window width
//     let newFontSize = window.innerWidth / 50; // Adjust as needed
    
//     // Ensure it doesn't exceed the maximum size
//     newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
    
//     topLeftSection.style.fontSize = `${newFontSize}px`;
// });


  const topMiddleSection = document.createElement('div');
  topMiddleSection.classList.add('top-middle-section');
  topMiddleSection.style.width = '100%';
  topMiddleSection.style.display = 'flex';
  topMiddleSection.style.justifyContent = 'space-between';
  topMiddleSection.style.alignItems = 'center';
  

 

//   window.addEventListener('resize', () => {
//     // Calculate new font size based on window width
//     let newFontSize = window.innerWidth / 50; // Adjust as needed
    
//     // Ensure it doesn't exceed the maximum size
//     newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
    
//     topMiddleSection.style.fontSize = `${newFontSize}px`;
// });


  topMiddleSection.style.fontWeight = 'bold';
 
 

  const topRightSection = document.createElement('div');
  topRightSection.classList.add('top-right-section');
  topRightSection.textContent = translations[language]['customerDetails'] || 'Customer details'; // Fallback to English if translation is not found
  //topRightSection.textContent = 'Customer details';
 
//   window.addEventListener('resize', () => {
//     // Calculate new font size based on window width
//     let newFontSize = window.innerWidth / 50; // Adjust as needed
    
//     // Ensure it doesn't exceed the maximum size
//     newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
    
//     topRightSection.style.fontSize = `${newFontSize}px`;
// });

  topRightSection.style.fontWeight = 'bold';

  topRow.appendChild(topLeftSection);
  topRow.appendChild(topMiddleSection);
  topRow.appendChild(topRightSection);


  //  Font size calculation function
  function updateFontSize() {
    let newFontSize = window.innerWidth / 50;
    newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
    topLeftSection.style.fontSize = `${newFontSize}px`;
    topMiddleSection.style.fontSize = `${newFontSize}px`;
    topRightSection.style.fontSize = `${newFontSize}px`;
  }
  
    //  Call it once on load
  updateFontSize();

  //  Call it on resize too
  window.addEventListener('resize', updateFontSize);

  const bottomRow = document.createElement('div');
  bottomRow.classList.add('bottom-row');

  const bottomLeftSection = document.createElement('div');
  bottomLeftSection.classList.add('bottom-left-section');

  const bottomMiddleSection = document.createElement('div');
  bottomMiddleSection.classList.add('bottom-middle-section');


  // Verify if we can select the element using the same selector
  const testSelector = bottomMiddleSection.querySelector('.initial-text');


  window.addEventListener('resize', () => {
    // Calculate new font size based on window width
    let newFontSize = window.innerWidth / 100; // Adjust as needed
    
    // Ensure it doesn't exceed the maximum size
    newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
    
    bottomMiddleSection.style.fontSize = `${newFontSize}px`;
});

  const bottomRightSection = document.createElement('div');
  bottomRightSection.classList.add('bottom-right-section');

  bottomRow.appendChild(bottomLeftSection);
  bottomRow.appendChild(bottomMiddleSection);
  bottomRow.appendChild(bottomRightSection);

  content.appendChild(topRow);
  content.appendChild(bottomRow);

  Chats_automatic[0] = bottomMiddleSection; // Map index to the middle bottom content element where chatboxes go
  rectangle_automatic[0] = bottomLeftSection;
  locations_automatic[0]= bottomRightSection;
  topMiddleButtons[0]=topMiddleSection;

  
  


 // socket.emit('createTabs', { tabs: tabsData });

}

//-------------------  END OF THIS FUNCTION TO CREATE DEFAULT TAB FOR AUTOMATIC MODE -----------------------------


// --------------------- RECORDING ---------------------------------


let mediaRecorder;
let recordedChunks = [];
let isRecording = false; // To track the recording state

// Function to start screen recording
function startScreenRecording() {
  console.log('Requesting screen capture...');
  navigator.mediaDevices.getDisplayMedia({ video: true })
    .then((stream) => {
      console.log('Screen capture granted, starting recording...');
      
      // Create a MediaRecorder for the screen stream
      mediaRecorder = new MediaRecorder(stream);
      recordedChunks = []; // Reset recorded chunks

      // Show a visual indication that recording has started
      const recordingStatus = document.createElement('p');
      recordingStatus.id = 'recordingStatus';
      recordingStatus.textContent = 'Recording has started...';
      document.body.appendChild(recordingStatus);

      // Store the recorded chunks in an array
      mediaRecorder.ondataavailable = (event) => {
        recordedChunks.push(event.data);
      };

      // Once recording is stopped, create a video file and trigger download
      mediaRecorder.onstop = () => {
        console.log('Recording stopped.');

        // Update the recording status message
        recordingStatus.textContent = 'Recording stopped. Downloading video...';

        // Create a Blob from the recorded chunks
        const videoBlob = new Blob(recordedChunks, { type: 'video/webm' });

        // Check if the videoBlob has data
        if (!videoBlob.size) {
          console.error('Video Blob is empty, no data recorded.');
          recordingStatus.textContent = 'Recording failed: No data recorded.';
          return;
        }

        // Create a URL for the Blob
        const videoUrl = URL.createObjectURL(videoBlob);

        // Automatically trigger file download
        const downloadLink = document.createElement('a');
        downloadLink.href = videoUrl;
        downloadLink.download = 'screen_recording.webm';  // Filename
        document.body.appendChild(downloadLink); // Add link to the DOM (required for Firefox)
        downloadLink.click(); // Trigger the download
        document.body.removeChild(downloadLink); // Remove link after clicking

        console.log('Video downloaded automatically.');
      };

      // Start the recording
      mediaRecorder.start();
      console.log('Recording started.');
      isRecording = true; // Update recording state
    })
    .catch((err) => {
      console.error('Error: ' + err);
      alert('Screen capture was denied or an error occurred: ' + err);
    });
}

// Function to stop screen recording
function stopScreenRecording() {
  if (mediaRecorder && isRecording) {
    mediaRecorder.stop();
    isRecording = false; // Update recording state
  }
}

// Listen for Space key press to toggle recording
document.addEventListener('keydown', (event) => {
  if (event.altKey && !event.repeat) {
    if (isRecording) {
      console.log('Control key pressed again, stopping and downloading...');
      stopScreenRecording();
    } else {
      console.log('Control key pressed, starting screen recording...');
      startScreenRecording();
    }
  }
});

// Stop recording when the page is closed
//window.addEventListener('beforeunload', () => {

  //stopScreenRecording();
//});


//     --------------                          Recording end         -------------------

document.addEventListener('DOMContentLoaded', () => {
  if (!manualMode) {
    // First frame: create DOM
    requestAnimationFrame(() => {
      createDefaultTabForAutomaticMode();

      // Second frame: safe to assume paint happened
      requestAnimationFrame(() => {
        console.log("Default tab painted for first load.");
        // Here you can safely run code that depends on visible DOM
      });
    });
  }
});



//let bottomMiddleSection;
let dotAnimation=null; // Variable to store the interval ID
let dotCount = 0; // Keeps track of the current number of dots
const maxDots = 3; // Maximum number of dots to display
const dotInterval = 500; // Interval for dot animation (in milliseconds)

function showInitialWaitingText() {
  // Assign bottomMiddleSection when the DOM is fully loaded
  bottomMiddleSection = document.querySelector('.bottom-middle-section');

  // Add initial text "Waiting for user queries..." only after bottomMiddleSection is assigned
  if (bottomMiddleSection) {
      const language = localStorage.getItem('language') || 'hu';

      const initialText = document.createElement('p'); // Create another paragraph for the main text
      initialText.style.marginTop = '30vh';
      initialText.style.marginLeft = '24vw';
      initialText.classList.add('initial-text'); // Add the class for styling

      // Set the translated waiting text (without dots)
      initialText.dataset.baseText = translations[language]['waiting'] || 'Waiting for user queries'; // Store base text

      // Apply initial text and font size
      initialText.textContent = initialText.dataset.baseText; 
      const newFontSize = Math.max(12, window.innerWidth / 70); // Adjust the divisor to control the scaling
      initialText.style.fontSize = `${newFontSize}px`;
      
      bottomMiddleSection.appendChild(initialText); // Append the main text

      // Start the dot animation
      startDotAnimation(initialText);
  } else {
      console.error("bottomMiddleSection not found after DOMContentLoaded.");
  }

}

document.addEventListener('DOMContentLoaded', () => {
  showInitialWaitingText()
});


// Function to start the dot animation with language support
function startDotAnimation(initialText) {
  // Clear any previous interval
  if (dotAnimation !== null) {
    clearInterval(dotAnimation);
    dotAnimation = null;
  }

  dotCount = 0; // Reset to start clean
  const language = localStorage.getItem('language') || 'hu';
  const baseText = translations[language]['waiting'] || 'Waiting for user queries';

  dotAnimation = setInterval(() => {
    dotCount = (dotCount + 1) % (maxDots + 1); // 0 to 3
    initialText.textContent = baseText + '.'.repeat(dotCount);
  }, dotInterval);
}


function clearInitialContent() {
    // Ensure bottomMiddleSection is defined before accessing it
    if (typeof bottomMiddleSection === 'undefined' || bottomMiddleSection === null) {
        return; // Exit the function early if it's not available
    }

   

    const initialContent = bottomMiddleSection.querySelector('.initial-text');
    if (initialContent) {
        initialContent.remove();
        clearInterval(dotAnimation); // Stop the dot animation
    } else {
        console.log("Selector test failed, element not found.");
    }
}





//------------   CREATING DROP DOWNS TO THE INPUT FIELDS FOR TABSCREATION ------------
//------------   CREATING DROP DOWNS TO THE INPUT FIELDS FOR TABSCREATION ------------
//------------   CREATING DROP DOWNS TO THE INPUT FIELDS FOR TABSCREATION ------------




const selectedNames = new Set(); 


window.addEventListener('DOMContentLoaded', () => {

  const inputField = document.getElementById('colleagues');
  const dropdown = document.getElementById('dropdown-container');
  let allUserNames = [];
  let dropdownReady = false;
  
  // Fetch user names from the server
  async function fetchUserNames() {
    try {
      const response = await fetch('/get_users');
      const usersData = await response.json();
  
      // Log to check the fetched data
      console.log('Fetched users data:', usersData);
  
      // Extract names from the response and store them
      allUserNames = usersData;
      renderDropdown();
      dropdownReady = true;
    } catch (error) {
      console.error('Error fetching user names:', error);
    }
  }
  
  function renderDropdown() {
    dropdown.innerHTML = '';  // Clear previous items
  
    allUserNames.forEach(user => {
      const option = document.createElement('div');
      option.className = 'dropdown-item';
      option.setAttribute('data-user-id', user.id);  // Add user ID for lookup later
      option.onclick = () => toggleName(user.name, option);
  
      option.innerHTML = `
        <span class="online-dot" style="background-color: ${user.is_online ? 'green' : 'gray'};"></span>
        ${user.name}
      `;
  
      dropdown.appendChild(option);
    });
  }
  
  
  socket.on('user_online_status_changed', function (data) {
    const { user_id, is_online } = data;
  
    // Make sure dropdown is populated
    if (!dropdownReady) {
      console.log('Dropdown not ready, skipping update');
      return;
    }
  
    // Find the corresponding dropdown item
    const option = dropdown.querySelector(`[data-user-id="${user_id}"]`);
    if (option) {
      const dot = option.querySelector('.online-dot');
      if (dot) {
        dot.style.backgroundColor = is_online ? 'green' : 'gray';
      }
    }
  });
  
  
  
  // Toggle name selection when clicking on an option
  function toggleName(name, option) {
    if (selectedNames.has(name)) {
      selectedNames.delete(name);  // Deselect if already selected
      option.classList.remove('selected');
    } else {
      selectedNames.add(name);  // Select if not already selected
      option.classList.add('selected');
    }
    updateInputField();
  }

  function updateInputField() {
    const namesArray = Array.from(selectedNames);
    document.getElementById('colleagues').value = namesArray.join(', ');
    input.value = namesArray.join(', ');
  
  // Manually emit the update since 'input' event won't fire
    socket.emit('update_colleagues_input', {input_value:input.value, timestamp: new Date().toISOString()});
  
  
  }
  // A betűnénti emission 3762-es sorban van
  
  
  
  
  
  
  let justOpened = false;

  inputField.addEventListener('focus', () => {
    if (inputField.value.trim() === '') {
      dropdown.classList.add('visible');
      justOpened = true;
      setTimeout(() => {
        justOpened = false;
      }, 100); // short delay is enough
    }
  });
  
  document.addEventListener('mousedown', (event) => {
    // Delay slightly to let focus settle
    requestAnimationFrame(() => {
      if (!inputField.contains(event.target) && !dropdown.contains(event.target)) {
        dropdown.classList.remove('visible');
      }
    });
  });

  // Prevent dropdown from closing if clicked inside
  dropdown.addEventListener('click', (event) => {
    event.stopPropagation();
  });
  
  
  // Call this function to populate the names dropdown on page load
  fetchUserNames();
  
  
  
  });


//------------   END OF CREATING DROP DOWNS TO THE INPUT FIELDS FOR TABS CREATION ------------















//                          FUNCTION TO CREATE MULTIPLE TABS FOR ADMIN USERS 


function createTabs() {
  if (!manualMode) return; // Do nothing if not in manual mode

  const inputField = document.getElementById('colleagues')
  
  // const input=inputField.value.trim();
  // const names = input.split(',').map(name => name.trim()).filter(name => name !== '');
  // socket.emit('createTabs', { names });

  let names = [];

  // Use selectedNames from dropdown if not empty
  if (selectedNames.size > 0) {
    names = Array.from(selectedNames);
    inputField.value = names.join(', '); // Optional: reflect it in input
  } else {
    const input = inputField.value.trim();
    names = input.split(',').map(name => name.trim()).filter(name => name !== '');
  }

  // If still empty, exit
  if (names.length === 0) return;
  
  
  const tabsData = [];
  isTabCreated=true;
  const language = localStorage.getItem('language') || 'hu';
  // Clear previous tabs and contents
  tabsContainer.innerHTML = '';
  tabContentsContainer.innerHTML = '';

  // Clear the chats map
  Object.keys(colleaguesChats).forEach(key => delete colleaguesChats[key]);
  names.forEach((name, index) => {
    const uniqueId = `tab-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    // Add tab data to the array
    tabsData.push({ name, uniqueId });
    // Create a tab
    const tab = document.createElement('div');
    tab.classList.add('tab');
    tab.classList.add('clickable');
    tab.dataset.tabId = uniqueId;
    tab.dataset.name = name;

    // Set display style to flex to align items in a row
    tab.style.display = 'inline-flex';
    tab.style.alignItems = 'center';  // Vertically center the items

    const tabName = document.createElement('span');
    tabName.textContent = name;
    tabName.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
    tab.appendChild(tabName);

    const exclamationMark = document.createElement('span');
    exclamationMark.classList.add('exclamation-mark');
    exclamationMark.textContent = '!';
    exclamationMark.style.color = 'red';
    exclamationMark.style.display = 'none'; // Initially hidden
    exclamationMark.style.marginLeft = '8px'; // Adjust spacing
    exclamationMark.style.fontWeight = 'bold';
    exclamationMark.style.fontSize = 'clamp(7px, 0.8vw, 12px)';

    tab.appendChild(exclamationMark);

    tab.onclick = () => showTab(uniqueId);

    // Enable double-click to rename the tab
    tab.ondblclick = () => {
      const currentName = tabName.textContent;

      // Create an input field to replace the tab name
      const input = document.createElement('input');
      input.type = 'text';
      input.value = currentName;
      input.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
      input.style.width = '100%'; // Adjust to fit tab width
      input.style.border = 'none';
      input.style.outline = 'none';
      input.style.background = 'transparent';
      input.style.color = 'inherit';
      input.style.textAlign = 'center';

      // Replace the tab name with the input field
      tab.replaceChild(input, tabName);

      // Focus and select the input field
      input.focus();
      input.select();

      // Save the new name when the input loses focus or Enter is pressed
      const saveName = () => {
        const newName = input.value.trim();

        // If the name is not empty, update it
        if (newName) {
          tabName.textContent = newName;

          // Update the tabsData array for this tab
          const tabData = tabsData.find(t => t.uniqueId === uniqueId);
          if (tabData) tabData.name = newName;

          socket.emit('update_tab_name', { uniqueId, newName });
        }

        // Replace the input field with the updated name
        tab.replaceChild(tabName, input);
      };

      // Handle saving when Enter is pressed
      input.onkeypress = (e) => {
        if (e.key === 'Enter') saveName();
      };

      // Handle saving when focus is lost
      input.onblur = saveName;
    };

    if (index === 0) tab.classList.add('active'); // Set the first tab as active
    tabsContainer.appendChild(tab);

   

    // Create tab content
    const content = document.createElement('div');
    content.classList.add('tab-content');
    content.dataset.tabIndex = uniqueId; // Associate content with tab index

    // Initially, hide all tab contents except the first
    if (index !== 0) content.style.display = 'none';

    // Create Grid Layout for Tab Content with 2 rows and 3 columns
    const topRow = document.createElement('div');
    topRow.classList.add('top-row');

    const language = localStorage.getItem('language') || 'hu';
    const topLeftSection = document.createElement('div');
    topLeftSection.classList.add('top-left-section');
    // topLeftSection.textContent = 'Customers';
    topLeftSection.textContent = translations[language]['customers'] || 'Ügyfelek';
    topLeftSection.style.fontWeight = 'bold'; 
    // topLeftSection.style.fontSize = '1.2vw';
    

    const topMiddleSection = document.createElement('div');
    topMiddleSection.classList.add('top-middle-section');
    //topMiddleSection.textContent = 'Chats';
    topMiddleSection.textContent = language === 'hu' ? 'Üzenetek' : 'Chats';
    topMiddleSection.style.fontWeight = 'bold';
    //topMiddleSection.style.fontSize = '1.2vw';
    
   

    const topRightSection = document.createElement('div');
    topRightSection.classList.add('top-right-section');
    topRightSection.textContent = translations[language]['customerDetails'] || 'Customer details'; // Fallback to English if translation is not found
  
    // topRightSection.textContent = 'Customer details';
    topRightSection.style.fontWeight = 'bold';
    //topRightSection.style.fontSize = '1.2vw';

    window.addEventListener('resize', () => {
      // Calculate new font size based on window width
      let newFontSize = window.innerWidth / 50; // Adjust as needed
      
      // Ensure it doesn't exceed the maximum size
      newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
      topLeftSection.style.fontSize = `${newFontSize}px`;
      topMiddleSection.style.fontSize = `${newFontSize}px`;
      topRightSection.style.fontSize = `${newFontSize}px`;
    });
    

    topRow.appendChild(topLeftSection);
    topRow.appendChild(topMiddleSection);
    topRow.appendChild(topRightSection);

    const bottomRow = document.createElement('div');
    bottomRow.classList.add('bottom-row');

    const bottomLeftSection = document.createElement('div');
    bottomLeftSection.classList.add('bottom-left-section');

    const bottomMiddleSection = document.createElement('div');
    bottomMiddleSection.classList.add('bottom-middle-section');

    const bottomRightSection = document.createElement('div');
    bottomRightSection.classList.add('bottom-right-section');

    bottomRow.appendChild(bottomLeftSection);
    bottomRow.appendChild(bottomMiddleSection);
    bottomRow.appendChild(bottomRightSection);

    content.appendChild(topRow);
    content.appendChild(bottomRow);
    tabContentsContainer.appendChild(content);
    colleaguesChats[uniqueId] = bottomMiddleSection; // Map index to the middle bottom content element where chatboxes go
    rectangle[uniqueId] = bottomLeftSection;
    locations[uniqueId]= bottomRightSection
    counterForManualModeAddMessage[uniqueId]={}
  });
  socket.emit('createTabs', { tabs: tabsData, frontend_time: new Date().toISOString() });
  document.querySelectorAll('.dropdown-item.selected').forEach(item => {
    item.classList.remove('selected');
  });
  inputField.value = '';
  selectedNames.clear();
  dropdown.classList.remove('visible');
  socket.emit('clear_input_field', { frontend_time: new Date().toISOString() });

  
  
}

//-------------------  END OF FUNCTION TO CREATE MULTIPLE TABS FOR ADMIN USERS -----------------------------


function showTab(uniqueId) {
  
  const tabs = document.querySelectorAll('.tabs div');
  
  const contents = document.querySelectorAll('.tab-content');
  
  // Remove the active class from all tabs and hide all contents
  tabs.forEach(tab => tab.classList.remove('active'));
  
  contents.forEach(content => content.style.display = 'none');

  // Add the active class to the selected tab
  const tab = document.querySelector(`.tab[data-tab-id="${uniqueId}"]`);
  if (tab) {
    // Remove the 'active' class from all tabs
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));

    // Add the 'active' class to the selected tab
    tab.classList.add('active');
  }
  
  // Find the active tab content
  const activeContent = document.querySelector(`.tab-content[data-tab-index="${uniqueId}"]`);
  if (activeContent) {
    activeContent.style.display = 'flex'; // Show the active content
  }


  // Retrieve user rectangles and location boxes for this tab
  const userRectangles = document.querySelectorAll(`.user-rectangle[data-tab-index="${uniqueId}"]`);
  const locations = document.querySelectorAll(`.location-box[data-tab-index="${uniqueId}"]`);

  locations.forEach(location => location.style.display = 'none');

  // Logic to determine which user rectangle should be selected
  let activeselectedUserId;
  if (activeRectangles[uniqueId]) {
    activeselectedUserId = activeRectangles[uniqueId].dataset.userId;
  } else {
    activeselectedUserId = false;
  }

  if (!activeselectedUserId && userRectangles.length > 0) {
    selectedUserId = userRectangles[userRectangles.length - 1].dataset.userId;
    const locationBoxToShow = document.querySelector(`.location-box[data-tab-index="${uniqueId}"][data-user-id="${selectedUserId}"]`);
    if (locationBoxToShow) {
      locationBoxToShow.style.display = 'block';
    }
  } else {
    const locationBoxToShow = document.querySelector(`.location-box[data-tab-index="${uniqueId}"][data-user-id="${activeselectedUserId}"]`);
    if (locationBoxToShow) {
      locationBoxToShow.style.display = 'block';
    }
  }
}









//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE
//                                       APPEND MESSAGE



function formatTimestampForClient(utcTimestamp) {
    if (!utcTimestamp) return '';

    const language = localStorage.getItem('language') || 'hu';
    const dt = new Date(utcTimestamp); // parse UTC ISO string

    const options = {
        timeZone: clientTimezone,
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    };

    // Force 24-hour format for Hungarian
    if (language === 'hu') {
        options.hour12 = false;
    }

    return dt.toLocaleString('default', options);
}



async function appendMessage(message) {
  
  const formattedTime = formatTimestampForClient(message.timestamp);

  //Convert timestamp to UTC Date
  if (message.timestamp) {
      if (typeof message.timestamp === 'number') {
          // Backend sent UNIX timestamp (in seconds)
          const messageDateUTC = new Date(message.timestamp * 1000);
          if (!isNaN(messageDateUTC)) {
              message.timestamp = messageDateUTC.toISOString();
          } else {
              console.warn("Invalid UNIX timestamp, leaving as is");
          }
      } else if (typeof message.timestamp === 'string') {
          // Backend sent ISO string, check if valid
          const testDate = new Date(message.timestamp);
          if (!isNaN(testDate)) {
              // Already valid ISO, leave as is
              message.timestamp = testDate.toISOString(); // normalize just in case
          } else {
              console.warn("Invalid timestamp string, leaving as is");
          }
      } else {
          console.warn("Unknown timestamp type, leaving as is");
      }
  } else {
      console.warn("No timestamp found in message");
  }

  let chatBox = null;
  let tabContentForChatBox = null;
  if (manualMode) {
    const language = localStorage.getItem('language') || 'hu';
    const translation_Sent_User = {
      sentAT: language === 'hu' ? 'A Küldés ideje' : 'Sent at',
      User: language === 'hu' ? 'Ügyfél' : 'User',
      };
    
    // Determine the tab index to distribute the message
    let tabIndex=null;
    for (const index in colleaguesChats) {
      const tabContent0 = colleaguesChats[index];
      const chatContainerInTab0 = tabContent0.querySelector(`.chat-container[data-user-id="${message.user_id}"]`);
      if (chatContainerInTab0) {
        tabIndex=index;
        break;
      }
    }
    if (!tabIndex){
      // tabIndex = messageCount % Object.keys(colleaguesChats).length;
      // messageCount++;
      const uniqueIds = Object.keys(colleaguesChats); // Get all uniqueIds
      const selectedIndex = messageCount % uniqueIds.length; // Round-robin logic
      tabIndex = uniqueIds[selectedIndex];  // Select the uniqueId based on the round-robin index
      messageCount++;  // Increment the message count for the next round-robin cycle
      // Emit the distribution to the server

    
     
      
    }
  
    const messageTotal = message.total_messages || 1;
    sentMessageCount++;
    isBatchMode = messageTotal > 1;
    console.log("*** BATCH MODE ***", isBatchMode)

    const emitData = {
        message: message,
        tab_uniqueId: tabIndex,
        total_messages: messageTotal
    };

    if (isBatchMode) {
        socket.emit('store_message_to_redis', { emitData: emitData, timestamp: new Date().toISOString() });
    } else {
        socket.emit('log_message_distribution', { 
            message: message, 
            tab_uniqueId: tabIndex, 
            timestamp: new Date().toISOString() 
        });
    }
    if (isBatchMode && sentMessageCount === totalMessagesToSend) {
      isBatchMode = false;
      totalMessagesToSend = 0;
      sentMessageCount = 0;
    }




  

    
    // Get the bottom-left section for the current tab index
    if (!rectangle[tabIndex]) {
        console.error(`rectangle[tabIndex] is undefined for tabIndex: ${tabIndex}`);
    }
    const bottomLeftSection = rectangle[tabIndex];
    
    // Check if a rectangle for the user already exists
    let userRectangle = document.querySelector(`.user-rectangle[data-user-id="${message.user_id}"]`);

    if (!userRectangle) {
      // Create a new rectangle for the user in the bottom-left section
      userRectangle = document.createElement('div');
      userRectangle.className = 'user-rectangle';
      userRectangle.dataset.userId = message.user_id;
      userRectangle.dataset.tabIndex = tabIndex;
      userRectangle.style.height = '50px';
      userRectangle.style.width = '100%'; // Assume it fills the width of the first column
      if (message.flag) {
        userRectangle.dataset.flag = 'true';
        userRectangle.classList.add('default-green');
      } else {
        userRectangle.dataset.flag = 'false';
        userRectangle.classList.add('default-blue');
      }
     
      userRectangle.style.borderBottom = '1px solid rgb(65, 75, 95)';
      userRectangle.style.cursor = 'pointer';
      userRectangle.style.display = 'flex';
      userRectangle.style.alignItems = 'center';
      userRectangle.style.padding = '0 10px';
      userRectangle.style.boxSizing = 'border-box';
      userRectangle.style.flexShrink = '0';
      const arrivalTime = new Date().toISOString(); // Get the current time in ISO format
      userRectangle.setAttribute('data-arrival-time', arrivalTime);
      
      const truncatedUserId = message.user_id.substring(0, 8);
      userRectangle.textContent = ` ${truncatedUserId}`;
      const colorUser = getPastelColor();
      //userRectangle.innerHTML = `<i class="fa-solid fa-user" style="margin-right: 10px; font-size: clamp(8px, 1.2vw, 15px); color: ${colorUser}"></i> ${truncatedUserId}`;
      userRectangle.innerHTML = `
        <div style="display: flex; align-items: center;">
          <i class="fa-solid fa-user"
            style="margin-right: 10px;
                    font-size: clamp(8px, 1.2vw, 15px);
                    color: ${colorUser};"></i>
          <span class="truncated-user-id "style="
            font-size: clamp(10px, 1.2vw, 15px);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          ">
            ${truncatedUserId}
          </span>
        </div>`;
      if (message.bot_message === 'Awaiting Admin Response...' ||
        message.bot_message === 'Adminisztrátori válaszra várakozás...') {
        userRectangle.classList.add('awaiting-response');
      }

      // Add drag-and-drop events
      userRectangle.draggable = true; // Enable native dragging
      userRectangle.addEventListener('dragstart', (e) => {
        const selectedIds = Array.from(document.querySelectorAll('.ctrl-click-selected'))
          .map(rect => rect.dataset.userId);

        const userIdsToDrag = selectedIds.length > 0 ? selectedIds : [message.user_id]; // Handle multiple or single
        e.dataTransfer.setData('text/plain', JSON.stringify({ userIds: userIdsToDrag, fromTab: tabIndex }));
        userRectangle.style.opacity = '1';
      });



      document.addEventListener('dragenter', (e) => {
        if (e.target.classList.contains('tab')) {
          e.preventDefault();
          e.target.classList.add('highlight');
        }
      });
      
      document.addEventListener('dragover', (e) => {
        if (e.target.classList.contains('tab')) {
          e.preventDefault();
        }
      });
      
      document.addEventListener('dragleave', (e) => {
        if (e.target.classList.contains('tab')) {
          e.target.classList.remove('highlight');
        }
      });
      
      document.addEventListener('drop', (e) => {
        if (e.target.classList.contains('tab')) {
          e.preventDefault();
          e.target.classList.remove('highlight');
      
          const data = JSON.parse(e.dataTransfer.getData('text/plain'));
          const fromTab = data.fromTab;
          const userIds = data.userIds;
          const toTab = e.target.dataset.tabId; // Assuming tabs have a `data-tabId`
      
          if (fromTab !== toTab) {
            // Sort userIds based on their arrival time (earliest first)
            userIds.sort((a, b) => {
              const rectA = document.querySelector(`.user-rectangle[data-user-id="${a}"]`);
              const rectB = document.querySelector(`.user-rectangle[data-user-id="${b}"]`);
      
              // Get arrival times, falling back to current time if not found
              const timeA = rectA ? new Date(rectA.dataset.arrivalTime).getTime() : Date.now();
              const timeB = rectB ? new Date(rectB.dataset.arrivalTime).getTime() : Date.now();
      
              return timeA - timeB; // Sort in ascending order (earliest first)
            });
      
            // Now handle the rectangles in order of arrival time
            userIds.forEach(userId => {
              const rectangleToMove = document.querySelector(`.user-rectangle[data-user-id="${userId}"]`);
              if (rectangleToMove) {
                // Use existing function for individual rectangles
                manageRectangleDragAndDrop(userId, fromTab, toTab);
                rectangleToMove.classList.remove('ctrl-click-selected');
                rectangleToMove.style.background = rectangleToMove.dataset.originalBackground;
              
              }
            });
            checkAwaitingResponse(fromTab);
            checkAwaitingResponse(toTab);
          }
        }
      });



      // Add click event to display the user's chatbox in the middle section
      userRectangle.addEventListener('click', (event) => {
        const userId = userRectangle.dataset.userId;
        if (event.ctrlKey) {
            // Ctrl + Click detected
         
            

            // Toggle red background on Ctrl + Click
            if (userRectangle.classList.contains('ctrl-click-selected')) {
                userRectangle.classList.remove('ctrl-click-selected');
                if (userRectangle.getAttribute('data-flag')=== 'true'){
                  userRectangle.classList.remove('user-rectangle-hover-lightgreen');
                  userRectangle.classList.add('default-green');
                }else{
                  userRectangle.classList.remove('user-rectangle-hover-lightblue');
                  userRectangle.classList.add('default-blue');
                }
                
            } else {
                userRectangle.classList.add('ctrl-click-selected');
               
                if (userRectangle.getAttribute('data-flag')=== 'true'){
                  userRectangle.classList.remove('default-green');
                  userRectangle.classList.add('user-rectangle-hover-lightgreen');
                }else{
                  userRectangle.classList.remove('default-blue');
                  userRectangle.classList.add('user-rectangle-hover-lightblue');
                }
               
                showUserChatBox(message.user_id, tabIndex);
                showLocationBox(message.user_id, tabIndex);
            
            }

            event.preventDefault();
            event.stopPropagation();
            return; // Prevent further logic
            }

  
        // Regular click logic
        const currentActiveRectangle = activeRectangles[tabIndex];
        if (currentActiveRectangle) {
          if (currentActiveRectangle !== userRectangle){
            if (currentActiveRectangle.getAttribute('data-flag')=== 'true'){   
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightgreen');
              currentActiveRectangle.classList.add('default-green');
            }else{
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightblue');
              currentActiveRectangle.classList.add('default-blue');
            }
            if (userRectangle.getAttribute('data-flag')=== 'true'){
              userRectangle.classList.remove('default-green');
              userRectangle.classList.add('user-rectangle-hover-lightgreen');
            }else{
              userRectangle.classList.remove('default-blue');
              userRectangle.classList.add('user-rectangle-hover-lightblue');
            }
            showUserChatBox(message.user_id, tabIndex);
            showLocationBox(message.user_id, tabIndex);
            activeRectangles[tabIndex] = userRectangle;
            isUserRectangleClickedPerTab[tabIndex] = true;
            
          }
          if(currentActiveRectangle === userRectangle){
            if (currentActiveRectangle.getAttribute('data-flag')=== 'true'){
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightgreen');
              userRectangle.classList.add('default-green');
            }else{
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightblue');
              userRectangle.classList.add('default-blue');
            }
            activeRectangles[tabIndex] = null;
            isUserRectangleClickedPerTab[tabIndex] = false;
            
          }   
        }else{
          activeRectangles[tabIndex] = userRectangle;
          if (userRectangle.getAttribute('data-flag')=== 'true'){
            userRectangle.classList.remove('default-green');
            userRectangle.classList.add('user-rectangle-hover-lightgreen');
          }else{
            userRectangle.classList.remove('default-blue');
            userRectangle.classList.add('user-rectangle-hover-lightblue');
          }

          showUserChatBox(message.user_id, tabIndex);
          showLocationBox(message.user_id, tabIndex);
          isUserRectangleClickedPerTab[tabIndex] = true;
          
        }
      });


      userRectangle.addEventListener('mouseover', () => {
        // Only apply hover if this rectangle is not the active one
        if (!activeRectangles || activeRectangles[tabIndex] == null) {
          if (message.flag) {
            userRectangle.classList.remove('default-green');
            userRectangle.classList.add('user-rectangle-hover-lightgreen');
        } else {
          userRectangle.classList.remove('default-blue');
          userRectangle.classList.add('user-rectangle-hover-lightblue');
          }
      }
  
      // Only apply hover if this rectangle is not the active one
      if (userRectangle !== activeRectangles?.[tabIndex]) {
          if (message.flag) {
              userRectangle.classList.remove('default-green');
              userRectangle.classList.add('user-rectangle-hover-lightgreen');
          } else {
              userRectangle.classList.remove('default-blue');
              userRectangle.classList.add('user-rectangle-hover-lightblue');
          }
      }
  });
        userRectangle.addEventListener('mouseout', () => {
          // Only remove hover if this rectangle is not the active one
          if (userRectangle !== activeRectangles[tabIndex]) {
            if (!userRectangle.classList.contains('ctrl-click-selected')){
              if (message.flag){
                userRectangle.classList.remove('user-rectangle-hover-lightgreen');
                userRectangle.classList.add('default-green');
              }else{
                userRectangle.classList.remove('user-rectangle-hover-lightblue');
                userRectangle.classList.add('default-blue');
              }
            
            }
             
              
          }
      });
          // Append the rectangle to the top of the bottom-left section
          bottomLeftSection.prepend(userRectangle); // Use prepend to add to the top
          // if(isUserRectangleClickedPerTab[tabIndex]){
          //   bottomLeftSection.scrollTop = bottomLeftSection.scrollHeight;
          // } 
      
    }




//////////////      CHATBOX  PART   ///////////////
//////////////      CHATBOX  PART   ///////////////
//////////////      CHATBOX  PART   ///////////////
//////////////      CHATBOX  PART   ///////////////
//////////////      CHATBOX  PART   ///////////////




    // Logic to create or find the user's chat container in the bottom-middle section
    let existingChatContainer = null;
    
    for (const index in colleaguesChats) {
      const tabContent = colleaguesChats[index];
      const chatContainerInTab = tabContent.querySelector(`.chat-container[data-user-id="${message.user_id}"]`);
      if (chatContainerInTab) {
        existingChatContainer = chatContainerInTab;
        tabContentForChatBox = tabContent;
        
        break;
      }
    }


    if (!existingChatContainer) {
      tabContentForChatBox = colleaguesChats[tabIndex];
      existingChatContainer = createChatBox(message.user_id, tabContentForChatBox, tabIndex);
      existingChatContainer.querySelector('.chat-box').classList.remove('not-awaiting-response');
      existingChatContainer.querySelector('.chat-box').classList.add('awaiting-response');
    
      const chatContainers = tabContentForChatBox.querySelectorAll('.chat-container');
      if (chatContainers.length > 1) {
        existingChatContainer.style.display = 'none'; // Hide the new chat container
      } else {
        existingChatContainer.style.display = 'block'; // Show the first chat container
      }
      if (message.awaiting === false) {
        userRectangle.classList.remove('awaiting-response');
      }

    
    } 
    chatBox = existingChatContainer.querySelector('.chat-box');
    // Update message logic remains the same
    const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
      const adminResponseElement = el.querySelector('.admin-response');
      
      // régi: return adminResponseElement && adminResponseElement.textContent.includes('Awaiting Admin Response...');
      return adminResponseElement && 
      (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
       adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'));
    
      });

    //RÉGI if (awaitingMessageElement) {
    //   awaitingMessageElement.querySelector('.admin-response').textContent = message.admin_response
    //     ? `Admin: ${message.admin_response}`
    //     : 'Awaiting Admin Response...';




    


    ///------------   AWAITING FOR ADMIN REPLY BLOCK   -------            ------  AWAITING FOR ADMIN REPLY BLOCK ----------------
    ///------------   AWAITING FOR ADMIN REPLY BLOCK   --------           ------  AWAITING FOR ADMIN REPLY BLOCK --------------





    
    if (awaitingMessageElement) {
      const adminResponseElement = awaitingMessageElement.querySelector('.admin-response');
  
      if (adminResponseElement) {
          const awaitingText = language === 'hu' 
              ? 'Adminisztrátori válaszra várakozás...' 
              : 'Awaiting Admin Response...';
  
          adminResponseElement.textContent = message.admin_response
              ? `Admin: ${message.admin_response}`
              : awaitingText;
      }

      
      chatBox.classList.add('not-awaiting-response');
      chatBox.classList.remove('awaiting-response');
      userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
      counterForManualModeAddMessage[tabIndex][message.user_id]+=1;
     

      ///---------------    WAITING  BLOCK BUT USER SEND A NEW QUESTION ----------------
      
      if (!message.admin_response) {

        console.log("Újabbat küldött???")

        //Megkeressük az Adminisztrátori válaszra várakozás... divet és remove it
        const adminResponseElement = awaitingMessageElement.querySelector('.admin-response');
        if (adminResponseElement) {
            const parentAdminMessage = adminResponseElement.closest('.admin-message');
            if (parentAdminMessage) {
                parentAdminMessage.remove();
            }
        }
        const messageElement = document.createElement('div');
        messageElement.className = 'message';
        
        const timestamp = message.timestamp || '';
        const formattedTime = formatTimestampForClient(message.timestamp);
        const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
        // Create user message content and style it to align on the right
        //<span class="user-id">User ID: ${message.user_id}</span> taken out of timestamp
        const userMessageContent = `
          <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
            <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
            
            <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
          </div>
        `;
        // Create admin message content and style it to align on the left
        const adminMessageContent = `
          <div class="admin-message">
            ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : `<span class="admin-response">${awaitingText}</span>`}
          </div>
        `;

        // Append user message first (right-aligned) and admin message after (left-aligned)
        messageElement.innerHTML = userMessageContent + adminMessageContent;
        

/////     ------    INSERTING THE MESSAGE     -----------
       
        const hiddenTimestampSpan = document.createElement('span');
        hiddenTimestampSpan.className = 'hidden-timestamp';
        hiddenTimestampSpan.style.display = 'none';
        hiddenTimestampSpan.textContent = timestamp;
        messageElement.appendChild(hiddenTimestampSpan);

         // Chronologically insert message based on normalized timestamps
        const newMessageTimestamp = new Date(timestamp);
        const existingMessages = Array.from(chatBox.getElementsByClassName('message'));

        let inserted = false;
        for (const existingMessage of existingMessages) {
          const hiddenTs = existingMessage.querySelector('.hidden-timestamp');
          if (hiddenTs) {
            const existingTs = new Date(hiddenTs.textContent.trim());
            if (newMessageTimestamp < existingTs) {
              chatBox.insertBefore(messageElement, existingMessage);
              inserted = true;
              break;
            }
          }
        }

        if (!inserted) {
          chatBox.appendChild(messageElement);
        }

     



        chatBox.classList.add('awaiting-response');
        chatBox.classList.remove('not-awaiting-response');
        userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
        counterForManualModeAddMessage[tabIndex][message.user_id]=0;
      } 
      
      // THE FIRST MESSAGE or SECONDLINE MESSAGE from THE USER HAS ARRIVED

    } else {
        console.log("ITT???-e?")
        if (message.user_message){   // brand new user message
    
          const messageElement = document.createElement('div');
           messageElement.className = 'message';
           
       
          // Use message timestamp or fallback to current time
          const timestamp = message.timestamp || '';
          console.log("új message")
          console.log(timestamp)
          let headlineContent = "";
          let headlineText = language === 'hu' 
            ? "Átvett üzenet a következő kollégától:" 
            : "Messages from ";
          if (message.flag === "deleted" && message.message_number === 1) {
            headlineContent = `
                <div class="headline-message" style="background: linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7));">
                    ${headlineText} ${message.name}
                </div>
            `;

        }

       
          


        
          // Create user message content and style it to align on the right
          //<span class="user-id">User ID: ${message.user_id}</span> taken out after timestamp
          const userMessageContent = `
            <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
              <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
              
              <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
            </div>
          `;
          // Create admin message content and style it to align on the left
          // const adminMessageContent = `
          //   <div class="admin-message">
          //     ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : '<span class="admin-response">Awaiting Admin Response...</span>'}
          //   </div>
          // `;
          const awaitingAdminText = language === "hu" ? "Adminisztrátori válaszra várakozás..." : "Awaiting Admin Response...";
          const adminMessageContent = `
            <div class="admin-message">
              ${
                message.bot_message
                  ? message.bot_message === "Awaiting Admin Response..."
                  ? `<span class="admin-response">${language === "hu" ? "Adminisztrátori válaszra várakozás..." : "Awaiting Admin Response..."}</span>` // If bot_message is "Awaiting Admin Response...", no Bot: prefix
                    : `<span class="admin-response">Bot: ${message.bot_message}</span>` // Otherwise, include Bot: prefix
                  : message.admin_response
                    ? message.flag === "deleted"
                      ? `<span class="admin-response">${message.admin_response}</span>` // If message is deleted, no Admin: prefix
                      : `<span class="admin-response">Admin: ${message.admin_response}</span>` // Otherwise, include Admin: prefix
                    : `<span class="admin-response">${awaitingAdminText}</span>` // If neither bot_message nor admin_response, SHOW!!! "Awaiting Admin Response..."
              }
            </div>
          `;

          // Append user message first (right-aligned) and admin message after (left-aligned)
          messageElement.innerHTML =
          (headlineContent ? headlineContent : '') +
          userMessageContent +
          adminMessageContent;

          // Append hidden timestamp AFTER .innerHTML is set
          const hiddenTimestampSpan = document.createElement('span');
          hiddenTimestampSpan.className = 'hidden-timestamp';
          hiddenTimestampSpan.style.display = 'none';
          hiddenTimestampSpan.textContent = timestamp;
          messageElement.appendChild(hiddenTimestampSpan);


            
         // Insert based on timestamp ordering
          const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
          const insertBeforeIndex = chatMessages.findIndex(el => {
            const ts = el.querySelector('.hidden-timestamp');
            return ts && new Date(ts.textContent) > new Date(timestamp);
          });

          if (insertBeforeIndex === -1) {
            chatBox.appendChild(messageElement);
          } else {
            chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
          }




          chatBox.classList.add('awaiting-response');
          chatBox.classList.remove('not-awaiting-response');
          userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
          counterForManualModeAddMessage[tabIndex][message.user_id]=0;
          // Check if bot_message exists and is not "Awaiting Admin Response..."
          if (
            message.bot_message && 
            message.bot_message !== "Awaiting Admin Response..." && 
            message.bot_message !== "Adminisztrátori válaszra várakozás..."
          ) {
            // Valid bot response: Remove awaiting-response classes
            chatBox.classList.remove('awaiting-response');
            userRectangle.classList.remove('awaiting-response');
            counterForManualModeAddMessage[tabIndex][message.user_id]=1;
          }
        
          if (message.awaiting === false) {
            chatBox.classList.remove('awaiting-response');
            userRectangle.classList.remove('awaiting-response');
            counterForManualModeAddMessage[tabIndex][message.user_id]=1;
          }
          
          

        }else {   //ITT A MÁSODIK ADMIN MESSAGET ADJUK HOZZÁ AMIKOR MÁR NINCS KIÍRVA, HOGY ADMIN VÁLSZRA VÁRVA...
          const messageElement = document.createElement('div');
          messageElement.className = 'message';
        
          // Create the hidden timestamp span
          const timestamp = message.timestamp || '';
          console.log("second message")
          console.log(timestamp)

          const adminMessageContent = `
            <div class="admin-message">
              ${message.admin_response
                ? message.flag === "deleted"
                  ? `<span class="admin-response">${message.admin_response}</span>`  // If message is deleted, no Admin: prefix
                  : `<span class="admin-response">Admin: ${message.admin_response}</span>`  // Otherwise, include Admin: prefix
                : '<span class="admin-response"></span>'  // If no admin response, show "Awaiting Admin Response..."
              }
            </div>
          `;
          // Append user message first (right-aligned) and admin message after (left-aligned)
          messageElement.innerHTML = adminMessageContent;
          
          
          
          // Append real hidden timestamp element
          const hiddenTimestampSpan = document.createElement('span');
          hiddenTimestampSpan.className = 'hidden-timestamp';
          hiddenTimestampSpan.style.display = 'none';
          hiddenTimestampSpan.textContent = timestamp;
          messageElement.appendChild(hiddenTimestampSpan);

          // Insert into chatBox sorted by timestamp
          const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
          const insertBeforeIndex = chatMessages.findIndex(el => {
            const tsEl = el.querySelector('.hidden-timestamp');
            if (!tsEl) return false;
            const existingTs = new Date(tsEl.textContent.trim()).getTime();
            const newTs = new Date(timestamp.trim()).getTime();
            return existingTs > newTs
          });

          if (insertBeforeIndex === -1) {
            chatBox.appendChild(messageElement);
          } else {
            chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
          }

          if (message.awaiting === false) {
            counterForManualModeAddMessage[tabIndex][message.user_id] = 1;
          }
          
      }
    }
  
    checkAwaitingResponse(tabIndex)
   
    // Get the bottom-right section for the current tab index
    const bottomRightSection = locations[tabIndex];

    // Check if a location box for the user already exists
    let userLocationBox = bottomRightSection.querySelector(`.location-box[data-user-id="${message.user_id}"]`);

    if (!userLocationBox) {
      // Create a new location box for the user if it does not exist
      userLocationBox = document.createElement('div');
      userLocationBox.className = 'location-box';
      userLocationBox.dataset.userId = message.user_id;
      userLocationBox.dataset.tabIndex = tabIndex;
      userLocationBox.style.overflowY = 'auto';
      userLocationBox.style.padding = '10px';
      userLocationBox.style.marginBottom = '5px';
      bottomRightSection.appendChild(userLocationBox);

       // Define translations
       const translation_location = {
        userID: language === 'hu' ? 'Ügyfélazonosító' : 'User-ID',
        location: language === 'hu' ? 'Hely' : 'Location',
        longitude: language === 'hu' ? 'Hosszúság' : 'Longitude',
        latitude: language === 'hu' ? 'Szélesség' : 'Latitude'
        };
      

      // Update the content of the location box with user data, including null values
      userLocationBox.innerHTML =  `
      <div style="margin-bottom: 10px;">
        <div class="user-id-header">
          <i class="fa-solid fa-user" style="margin-right: 5px;"></i>
          <span class="userID_text">${translation_location.userID}</span>
        </div>
        <div class="paddingleft" style="margin-top: 5px; font-size: clamp(7px, 0.8vw, 12px);">
          <span class="full-user-id-locbox">${message.user_id !== null ? message.user_id : 'No Data'}</span>
          <span class="truncated-user-id-locbox">${message.user_id !== null ? message.user_id.substring(0, 8) : 'No Data'}</span>
        </div>
      </div>
      <div style="margin-bottom: 10px;">
        <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(8px, 1.2vw, 15px); border-radius: 4px;">
          <i class="fa-solid fa-location-dot" style="margin-right: 5px;"></i>
          <span class="location_userlocationbox">${translation_location.location}</span>
        </div>
        <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(7px, 0.8vw, 12px);">
          
          ${message.location ?? 'No Data'}
        </div>
      </div>
      
      <div id="location-map" style="width: 100%; height: 200px; margin-top: 10px;"></div>
      <div style="margin-bottom: 10px;">
        <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(5px, 1.2vw, 15px); border-radius: 4px;">
          <i class="fa-solid fa-arrows-left-right" style="margin-right: 5px;"></i>
          <span class="longitude_userlocationbox">${translation_location.longitude}</span>
        </div>
        <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
          
          ${message.longitude ?? 'No Data'}
        </div>
      </div>
      <div style="margin-bottom: 10px;">
        <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(5px, 1.2vw, 15px); bold; border-radius: 4px;">
          <i class="fa-solid fa-arrows-up-down" style="margin-right: 5px;"></i>  
          <span class="latitude_userlocationbox">${translation_location.latitude}</span>
        </div>
        <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
          
          ${message.latitude ?? 'No Data'}
        </div>
      </div>
      
    `;
  
      bottomRightSection.prepend(userLocationBox);
     // Initialize the map immediately after appending the content
      const mapContainer = userLocationBox.querySelector("#location-map");
      
      if (message.latitude !== null && message.longitude !== null) {
        const map = L.map(mapContainer).setView([message.latitude, message.longitude], 13);
        
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
          attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
  
        L.marker([message.latitude, message.longitude]).addTo(map)
          .bindPopup("Location: " + message.location)
          .openPopup();

        // Ensure Leaflet resizes properly
        setTimeout(() => {
          map.invalidateSize();
          }, 300);

          requestAnimationFrame(() => {
              map.invalidateSize();
          });

          // Observe element visibility and refresh map when shown
          const visibilityObserver = new IntersectionObserver(entries => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    map.invalidateSize();
                }
            });
        }, { threshold: 0.1 });

        visibilityObserver.observe(userLocationBox);

        // Handle window resize events
        L.DomEvent.on(window, 'resize', () => map.invalidateSize());

      } else {
         // Hide the map container if no valid coordinates exist
         userLocationBox.querySelector("#location-map").style.display = "none";
     }
    
    if (bottomRightSection.querySelectorAll('.location-box').length > 1) {
      userLocationBox.style.display = 'none';
    }
      
    }
    
   
    // userRectangle.addEventListener('click', () => {
    //   showLocationBox(message.user_id, tabIndex);
    // });

    // // Show the latest chat container in the middle section
    // showUserChatBox(message.user_id, tabIndex);
   
  } else {

  












    /////////////////////////////////                                 /////////////////////////////////
    //    AUTOMATIC      ////////////                                 //    AUTOMATIC      ////////////
    /////////////////////////////////                                 /////////////////////////////////
    
    




    /////////////////////////////////                                /////////////////////////////////
    //    AUTOMATIC      ////////////                                //    AUTOMATIC      ////////////
    /////////////////////////////////                                /////////////////////////////////

    clearInitialContent();
    
    console.log("-----------------1")
    
    const bottomLeftSection = rectangle_automatic[0];
    const bottomMiddleSection=Chats_automatic[0];
    const bottomRightSection=locations_automatic[0];
    const topMiddleSection=topMiddleButtons[0]
    const language = localStorage.getItem('language') || 'hu';
    const translation_Sent_User = {
      sentAT: language === 'hu' ? 'A Küldés ideje' : 'Sent at',
      User: language === 'hu' ? 'Ügyfél' : 'User',
      };
    
  
    if (!bottomLeftSection || !bottomRightSection) {
      console.error('Bottom sections are not found in the DOM.');
      return;
    }

     // Apply styles for layout

    if (automaticResponseStates[message.user_id]){
    

     }
    
    
    if (!userElements[message.user_id]&& message.user_id){
      // Create the 'Chats' label
      prependeduserId=message.user_id
      const buttonContainer = document.createElement('div');
      buttonContainer.classList.add('button-container');
      buttonContainer.style.display = 'flex';
      buttonContainer.dataset.userId = message.user_id;
      buttonContainer.style.justifyContent = 'space-between';
      buttonContainer.style.alignItems = 'center';
      buttonContainer.style.margin = '0 auto';
      buttonContainer.style.width = '100%';
     
      console.log("-----------------2")
      
      
      const chatsLabel = document.createElement('span');
      //chatsLabel.textContent = 'Chats';
      chatsLabel.setAttribute('data-lang', 'chats');
      chatsLabel.textContent = translations[language]['chats'] || 'Üzenetek';
      chatsLabel.style.fontWeight = 'bold';
      chatsLabel.style.marginLeft = '20px';
      
      
      
      // Create the 'Admin Response' button
      const adminResponseButton = document.createElement('button');
      adminResponseButton.classList.add('admin-intervention');
      adminResponseButton.textContent = translations[language]['automaticResponse'];
      // adminResponseButton.textContent = 'Admin Intervention';
      adminResponseButton.style.padding = '4px 10px';
      adminResponseButton.style.background = '#007bff'; // Button color (blue)
      adminResponseButton.style.color = 'white'; // Text color
      adminResponseButton.style.border = 'none';
      adminResponseButton.style.borderRadius = '30px';
      adminResponseButton.style.cursor = 'pointer';
      adminResponseButton.style.fontSize = '0.7em';
      adminResponseButton.style.marginRight = '20px';
      // Store user_id in the button for easy reference
      adminResponseButton.setAttribute('data-user-id', message.user_id);
      
      console.log("-----------------3")

      if (automaticResponseStates[message.user_id] === undefined) {
        automaticResponseStates[message.user_id] = false;  //check what is written on the button
    }
      // Function to update button styles based on the current state
      function updateButtonStyles(adminResponseButton, isAutomaticResponseNeeded) {
        if (isAutomaticResponseNeeded) {
            adminResponseButton.style.backgroundColor = '#007bff'; // Blue for Automatic Response
            adminResponseButton.onmouseover = () => adminResponseButton.style.backgroundColor = '#0056b3'; // Darker blue on hover
            adminResponseButton.onmouseout = () => adminResponseButton.style.backgroundColor = '#007bff'; // Original blue color
        } else {
            adminResponseButton.style.backgroundColor = '#28a745'; // Green for Admin Intervention
            adminResponseButton.onmouseover = () => adminResponseButton.style.backgroundColor = '#218838'; // Darker green on hover
            adminResponseButton.onmouseout = () => adminResponseButton.style.backgroundColor = '#28a745'; // Original green color
        }
    }

      // Initial setup for hover effect
      updateButtonStyles(adminResponseButton, automaticResponseStates[message.user_id]);
     
      console.log("-----------------4")
      // Click event to toggle button text and color + handling the waiting message if we should append or not
      adminResponseButton.onclick = () => {
        automaticResponseStates[message.user_id] = !automaticResponseStates[message.user_id];
        //const language = localStorage.getItem('language') || 'hu';
        userButtons[message.user_id].textContent = automaticResponseStates[message.user_id] ? translations[language]['manualIntervention'] : translations[language]['automaticResponse'];
        // userButtons[message.user_id].textContent = automaticResponseStates[message.user_id] ? 'Automatic Response' : 'Admin Intervention';
        updateButtonStyles(userButtons[message.user_id], automaticResponseStates[message.user_id]); // Update styles based on new state

        const userRectangle = rectangle_automatic[message.user_id];
        const avatar = userRectangle.querySelector('.human-avatar');


          // Immediate hover effect if hovering during click
          userButtons[message.user_id].style.backgroundColor = automaticResponseStates[message.user_id] ? '#0056b3' : '#218838';
      
        userButtonStates[message.user_id] = automaticResponseStates[message.user_id] ? 'automaticResponse' : 'adminIntervention';
        chatBox = Chats_automatic[message.user_id];
      

console.log("-----------------5")

        // Emit to server the rectangles' state if it is manual or automatic
        
        const timestamp = new Date().toISOString();  // UTC timestamp
        socket.emit('update_response_state', {
          user_id: message.user_id,
          state: automaticResponseStates[message.user_id],
          frontend_time: timestamp
        });
        //socket.emit('update_response_state', { user_id: message.user_id, state: automaticResponseStates[message.user_id] });
          // here we are on manual /user mode and here we want to display only the WAITING message:
        if(automaticResponseStates[message.user_id]){
          // Manual mode → show avatar
          avatar.style.display = "flex";

          switchToManualModeforOneUser(language)  // THIS CREATES THE IMPUTBOX FOR ADMINS IN AUTOMATIC - MANUAL MODE

          const messageElement = document.createElement('div');
          messageElement.className = 'message';
          
          // <span class="user-id">User ID: ${message.user_id}</span>
          const userMessageContent = `
            <div class="user-message" style="display: none; background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
              <span class="timestamp">${translation_Sent_User.sentAT}: ${message.timestamp}</span>
              
              <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
            </div>
          `;

console.log("-----------------6")

          // Determine translated text
          const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';

          // Create admin message content and style it to align on the left
          const adminMessageContent = `
            <div class="admin-message">
              <span class="admin-response">${awaitingText}</span>
            </div>
          `;
          
          // Get all messages in chatBox
          const messages = chatBox.querySelectorAll('.message');

          // Find the last .message element
          const lastMessage = messages.length > 0 ? messages[messages.length - 1] : null;

          let shouldAppend = true; // Default to appending
          
          if (lastMessage) {
              // Find .admin-message inside the last message
              const lastAdminMessage = lastMessage.querySelector('.admin-message');
             
              if (lastAdminMessage) {
                  // Find .admin-response inside .admin-message
                  const lastAdminResponse = lastAdminMessage.querySelector('.bot-response');
                  
                  if (lastAdminResponse) {
                      const lastAdminText = lastAdminResponse.textContent.trim();
                      
                      // If the last message is the awaiting response, don't append
                      if (lastAdminText === 'Adminisztrátori válaszra várakozás...' || lastAdminText === 'Awaiting Admin Response...') {
                          shouldAppend = false;
                      }
                  }
              }
          }

    console.log("-----------------6")
          // Append only if the last message is NOT the awaiting response message
          if (shouldAppend) {
              messageElement.innerHTML = userMessageContent + adminMessageContent;
              chatBox.appendChild(messageElement);
          }
          // Append user message first (right-aligned) and admin message after (left-aligned)
          // messageElement.innerHTML = userMessageContent + adminMessageContent;
        
          // chatBox.appendChild(messageElement);
          chatBox.classList.add('awaiting-response');
          userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
          counterForAddAdminMessage[message.user_id] = 0
        }else{
          // Automatic mode → hide avatar
            avatar.style.display = "none";
          removeAdminResponseControls(message.user_id)

          const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
          const botResponseElement = el.querySelector('.bot-response');
          const adminResponseElement = el.querySelector('.admin-response');
        
          // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
          return (botResponseElement && 
            (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
             botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
           (adminResponseElement && 
            (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
             adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
        });
        // We have awaiting message
        if (awaitingMessageElement) {
          awaitingMessageElement.remove();
        }
        chatBox.classList.remove('awaiting-response');
        userRectangle.classList.remove('awaiting-response');

console.log("-----------------7")
          

          
        }
        

      
        
          
        };
    
      buttonContainer.appendChild(chatsLabel);
      buttonContainer.appendChild(adminResponseButton);
  
      // Append the container to the topMiddleSection
      topMiddleSection.appendChild(buttonContainer);

      userElements[message.user_id] = buttonContainer;
      userButtons[message.user_id] = adminResponseButton;
      
      

    }
    
    console.log("-----------------8")

    // Check if a rectangle for the user already exists
    let userRectangle = rectangle_automatic[message.user_id];
    if (!userRectangle) {
      userRectangle = document.createElement('div');
      userRectangle.className = 'user-rectangle';
      userRectangle.dataset.userId = message.user_id;
      userRectangle.style.height = '50px';
      userRectangle.style.width = '100%';
 
      userRectangle.style.backgroundColor = getUserColor(message.user_id);
      userRectangle.style.borderBottom = '1px solid rgb(65, 75, 95)';
      userRectangle.style.cursor = 'pointer';
      userRectangle.style.display = 'flex';
      userRectangle.style.alignItems = 'center';
      userRectangle.style.padding = '0 10px';
      userRectangle.style.boxSizing = 'border-box';
      userRectangle.style.flexShrink = '0';
      userRectangle.textContent = message.user_id.substring(0, 8); // Truncated user ID
      const truncatedUserId = message.user_id.substring(0, 8); // Truncated user ID
      const color = getPastelColor();
      userRectangle.innerHTML = `
        <div style="display: flex; align-items: center;">
          <i class="fa-solid fa-user"
            style="margin-right: 10px;
                    font-size: clamp(8px, 1.2vw, 15px);
                    color: ${color};"></i>
          <span class="truncated-user-id" style="
            font-size: clamp(10px, 1.2vw, 15px);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            margin-right: 10px;
          ">
            ${truncatedUserId}
          </span>
        </div>
        <div class="human-avatar" style="display: none; margin-left: auto;">
          <span class="material-symbols-outlined" style="font-size: 20px; color: white;">
            support_agent
          </span>
        </div>
        
        `;
      
      console.log("-----------------9")
        
      
      // updateTopMiddleSection(message.user_id);
      //  // Track the currently active (clicked) rectangle

      // Mouseover (hover in) event logic
      userRectangle.addEventListener('mouseover', () => {
          // Only apply hover if this rectangle is not the active one
          if (userRectangle !== activeRectangle) {
              userRectangle.classList.add('user-rectangle-hover');
          }
      });

      // Mouseout (hover out) event logic
      userRectangle.addEventListener('mouseout', () => {
          // Only remove hover if this rectangle is not the active one
          if (userRectangle !== activeRectangle) {
              userRectangle.classList.remove('user-rectangle-hover'); // Remove hover class
              userRectangle.style.backgroundColor = getUserColor(message.user_id); // Revert to original color
          }
      });
       

console.log("-----------------10")
      /////////////////////////////////////////////
      //////////     CLICK     ///////////////////
      ///////////////////////////////////////////




      // Click event logic (also update the active rectangle)
      userRectangle.addEventListener('click', () => {
        
        // If there's an active rectangle, reset its background and hover class
        if (activeRectangle) {
            // Reset the previously active rectangle (regardless of whether it's the same as the clicked one)
            activeRectangle.classList.remove('user-rectangle-hover');
            activeRectangle.style.backgroundColor = getUserColor(activeRectangle.dataset.userId); // Reset previous active color
        }
    
        // Check if the same rectangle was clicked again
        if (activeRectangle === userRectangle) {
            // Reset the active rectangle and click state
            activeRectangle = null;
            isUserRectangleClicked = false;
    
            // Reset the chat visibility to the default behavior (latest message visible)
            resetChatVisibility();
            
        } else {
            // Set this rectangle as the new active one
            activeRectangle = userRectangle;
            isUserRectangleClicked = true;
            
            // Add hover class and set background color to the active rectangle
            userRectangle.classList.add('user-rectangle-hover'); // Keep hover class
            userRectangle.style.backgroundColor = 'hover-color'; // Apply the hover background color
    
            // Show the user's chat box and location
            showUserChatBox_Automatic(message.user_id, bottomMiddleSection);
            showLocationBox_Automatic(userRectangle.dataset.userId, bottomRightSection);
            showButton_Automatic(activeRectangle.dataset.userId, topMiddleSection)  
            
          }
    });
console.log("-----------------11")

      // bottomLeftSection.prepend(userRectangle);
      rectangle_automatic[message.user_id] = userRectangle; // Store the rectangle in the automatic mode object

      userRectangle.addEventListener('mouseover', () => {
        userRectangle.classList.add('user-rectangle-hover'); // Add hover class
      });
      
   

      // Append the rectangle to the top of the bottom-left section
      bottomLeftSection.prepend(userRectangle); // Use prepend to add to the top
      // if (!isUserRectangleClicked){
      //   bottomLeftSection.scrollTop = 0;
      // }
      
      // scroll to the bottom:
      // bottomLeftSection.scrollTop = bottomLeftSection.scrollHeight;
    }

console.log("-----------------12")  

    if (message.bot_message === 'Awaiting Admin Response...' ||
      message.bot_message === 'Adminisztrátori válaszra várakozás...') {
      userRectangle.classList.add('awaiting-response');
    }

    console.log(userRectangle)
    // Check if a chat box for the user already exists
    chatBox = Chats_automatic[message.user_id];
    if (!chatBox) {
      chatBox = createChatBox_automatic(message.user_id, bottomMiddleSection).querySelector('.chat-box_automatic');
      Chats_automatic[message.user_id] = chatBox;
       // Store the chat box in the automatic mode object
    }


    const chatContainers = bottomMiddleSection.querySelectorAll('.chat-container');
    if (!isUserRectangleClicked) {
      resetChatVisibility();
  }
    
console.log("-----------------13")  

      function resetChatVisibility() {
    chatContainers.forEach(container => {
        container.style.display = 'none'; // Hide all chat containers initially
    });

    // Assuming `message.user_id` contains the latest message's user ID
    chatContainers.forEach(container => {
        const userId = container.dataset.userId;

        if (userId === message.user_id) {
            container.style.display = 'block'; // Show the current user's chat container based on the latest message
            
        }
    });
  }
   
  


      /////////////////////////////////////////////////////////////////
      //////////     AUTOMATIC OR MANUAL / USER    ///////////////////
      ///////////////////////////////////////////////////////////////
  


      console.log("-----------------14")
  
  if (automaticResponseStates[message.user_id] || (!message.bot_message && message.admin_response)){  //automaticResponseStates tricky check what is written on the button if manual intervention needed it is automatic mode but the value is false as have manual intervention on the button
        console.log("%MESSSAGE CHECK%")
        console.log(message)
        console.log("MESSAGECHECK END")
        chatBox = Chats_automatic[message.user_id];
      
        // Update message logic remains the same
        const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
          const botResponseElement = el.querySelector('.bot-response');
          const adminResponseElement = el.querySelector('.admin-response');
        
          // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
          return (botResponseElement && 
            (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
             botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
           (adminResponseElement && 
            (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
             adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
        });
        // We have awaiting message
        if (awaitingMessageElement) {
       
          const adminResponseElement = awaitingMessageElement.querySelector('.admin-response');
          if (adminResponseElement) {
      
            const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
        
     console.log("-----------------15")
            adminResponseElement.textContent = message.admin_response
              ? `Admin: ${message.admin_response}`
              : awaitingText;
          }
          if (!message.admin_response) { //here we have scenario when awaiting admin response hasn't been answered, as automatic anwsered we had, but new message from chatapp arrived
            awaitingMessageElement.remove();
            chatBox.classList.add('awaiting-response');
            chatBox.classList.remove('not-awaiting-response');
            userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
            console.log("3")

            const messageElement = document.createElement('div');
            messageElement.className = 'message';

        
            const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
            
            const formattedTime = formatTimestampForClient(message.timestamp);
            // <span class="user-id">User ID: ${message.user_id}</span>  taken out of after timestamp
            // Create user message content and style it to align on the right
            const userMessageContent = `
              <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
                <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
               
                <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
              </div>
            `;
      console.log("-----------------16")
            // Create admin message content and style it to align on the left
            const adminMessageContent = `
              <div class="admin-message">
                ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : `<span class="admin-response">${awaitingText}</span>`}
              </div>
            `;
      
            
            // Append user message first (right-aligned) and admin message after (left-aligned)
            messageElement.innerHTML = userMessageContent + adminMessageContent;

            

            // Extract datetime using regex (YYYY-MM-DD hh:mm:ss)
            const tsString = message.timestamp || '';
         
            const hiddenTimestampSpan = document.createElement('span');
            hiddenTimestampSpan.className = 'hidden-timestamp';
            hiddenTimestampSpan.style.display = 'none';
            hiddenTimestampSpan.textContent = tsString;

       console.log("-----------------17")      

            messageElement.appendChild(hiddenTimestampSpan);


            // Get all existing messages
            const chatMessages = Array.from(chatBox.getElementsByClassName('message'));

            // Find the first message with a later timestamp
            const insertBeforeIndex = chatMessages.findIndex(el => {
              const ts = el.querySelector('.hidden-timestamp');
              return ts && new Date(ts.textContent) > new Date(tsString);
            });

            // Insert in chronological order
            if (insertBeforeIndex === -1) {
              chatBox.appendChild(messageElement);
            } else {
              chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
            }



       console.log("-----------------18")     

      
            //chatBox.appendChild(messageElement);
            //counterForAddAdminMessage[message.user_id] += 1;   // I think is not needed here !!!!! as we will have new awaiting...

          } else {
          
//       NORMAL FIRST MANUAL RESPONSE SECOND PART:

            chatBox.classList.add('not-awaiting-response');
            chatBox.classList.remove('awaiting-response');
            userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
            counterForAddAdminMessage[message.user_id] += 1;
          }
       
          const botResponseElement = awaitingMessageElement.querySelector('.bot-response');  ///??? maybe it is not necessary never we have it in manual mode
          if (botResponseElement) {
      
            const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';

            botResponseElement.textContent = message.admin_response
              ? `Admin: ${message.admin_response}`
              : awaitingText;
          }

        } else {

          console.log("-----------------19")
  //  THIS IS ACTIVATED WHEN WE ADD A SECOND MESSAGE MANUALLY:

          console.log("IDEBEJÖN?_1")
          if (message.manual_response){
          
            // Create admin message content and style it to align on the left
            const messageElement = document.createElement('div');
            messageElement.className = 'message';
            const adminMessageContent = `
            <div class="admin-message">
              ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : '<span class="admin-response"> </span>'}
            </div>
            `;
   console.log("-----------------20")   
            // Append user message first (right-aligned) and admin message after (left-aligned)
            messageElement.innerHTML = adminMessageContent;
      
            chatBox.appendChild(messageElement);
            counterForAddAdminMessage[message.user_id] = 1;

//  THIS IS ACTIVATED WHEN WE ADD A SECOND MESSAGE MANUALLY:

          }else{
          console.log("IDEBEJÖN?_2", message)
          const messageElement = document.createElement('div');
          messageElement.className = 'message';
          // Get the selected language
      
          const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';

          
          // Create user message content and style it to align on the right
          // <span class="user-id">User ID: ${message.user_id}</span>  taken out before timestamp
          const userMessageContent = `
            <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
              <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
           
              <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
            </div>
          `;
   console.log("-----------------21") 
          // Create admin message content and style it to align on the left
          const adminMessageContent = `
          <div class="admin-message">
            ${message.admin_response ? 
              `<span class="admin-response">Admin: ${message.admin_response}</span>` 
              : `<span class="admin-response">${awaitingText}</span>`}
          </div>
        `;
    
          // Append user message first (right-aligned) and admin message after (left-aligned)
          messageElement.innerHTML = userMessageContent + adminMessageContent;

          // Use message.timestamp if exists, otherwise use current time
          const tsString = message.timestamp || '';
                      
console.log("-----------------22")
          const hiddenTimestampSpan = document.createElement('span');
          hiddenTimestampSpan.className = 'hidden-timestamp';
          hiddenTimestampSpan.style.display = 'none';
          hiddenTimestampSpan.textContent = tsString;
          messageElement.appendChild(hiddenTimestampSpan);

          const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
          const insertBeforeIndex = chatMessages.findIndex(el => {
            const ts = el.querySelector('.hidden-timestamp');
            return ts && new Date(ts.textContent) > new Date(tsString);
          });
          // * itt egyszer hozááadódik
          // Insert in chronological order
          if (insertBeforeIndex === -1) {
            chatBox.appendChild(messageElement);
          } else {
            chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
          }



console.log("-----------------23")

    
          //chatBox.appendChild(messageElement);
          counterForAddAdminMessage[message.user_id] += 1;
          console.log("counterellenőrzés")
          console.log(counterForAddAdminMessage[message.user_id])
          
    
          // Add class based on the presence of an admin response
          if (!message.admin_response) {
            // **  ide jön
            console.log("7")
            chatBox.classList.add('awaiting-response');
            chatBox.classList.remove('not-awaiting-response');
            userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
            counterForAddAdminMessage[message.user_id] = 0;
       
//  THIS IS ACTIVATED WHEN WE ADD A SECOND MESSAGE MANUALLY:

          } else {
            console.log("8")
            chatBox.classList.add('not-awaiting-response');
            chatBox.classList.remove('awaiting-response');
            userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
            if (counterForAddAdminMessage[message.user_id]>0){
              console.log("counteradminmessage>0????")
              // Create admin message content and style it to align on the left
              const adminMessageContent = `
              <div class="admin-message">
                ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : '<span class="admin-response"> </span>'}
              </div>
              `;
    console.log("-----------------24")    
              // Append user message first (right-aligned) and admin message after (left-aligned)
              messageElement.innerHTML = adminMessageContent;

              // Use message.timestamp if exists, otherwise current time
              const tsString = message.timestamp || '';
                              

              const hiddenTimestampSpan = document.createElement('span');
              hiddenTimestampSpan.className = 'hidden-timestamp';
              hiddenTimestampSpan.style.display = 'none';
              hiddenTimestampSpan.textContent = tsString;
              messageElement.appendChild(hiddenTimestampSpan);

              console.log("HIDDEN TIMESTAMP:")
              console.log(hiddenTimestampSpan)

              // Find where to insert based on timestamp
              const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
              const insertBeforeIndex = chatMessages.findIndex(el => {
                const ts = el.querySelector('.hidden-timestamp');
                return ts && new Date(ts.textContent) > new Date(tsString);
              });

              // Insert in correct place or append if no later message
              if (insertBeforeIndex === -1) {
                chatBox.appendChild(messageElement);
              } else {
                chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
              }

              // * itt másodszor hozááadódik, de ha nincs usesr message akkor azt szimplán kicsréljük miert ugyanazt messageElement-et hazsnáljuk mint előbb
              
console.log("-----------------25")
              
              //chatBox.appendChild(messageElement);
       

            }
            counterForAddAdminMessage[message.user_id] += 1;
            
          }
        }
      }
console.log("-----------------26")

//   AUTOMATIC MODE:

  }else{
    console.log("9")
      const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
      
       
      
      // Create or update the message element in the chat box
      // <span class="user-id">User ID: ${message.user_id}</span>  taken out before timestamp
      const messageElement = document.createElement('div');
      messageElement.className = 'message';
      messageElement.innerHTML = `
          <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
            <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
          
            <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
          </div>
          ${message.bot_message ? `<div class="admin-message"><span class="bot-response">
            ${
          message.bot_message === awaitingText || message.bot_message === 'Awaiting Admin Response...' 
          ? awaitingText 
          : `Bot: ${message.bot_message}`}
           </span></div>` : ''}
      `;
console.log("-----------------27")
      // Use message.timestamp if exists, otherwise current time
      const tsString = message.timestamp || '';

      const hiddenTimestampSpan = document.createElement('span');
      hiddenTimestampSpan.className = 'hidden-timestamp';
      hiddenTimestampSpan.style.display = 'none';
      hiddenTimestampSpan.textContent = tsString;
      messageElement.appendChild(hiddenTimestampSpan);

      // Find where to insert based on timestamp
      const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
      const insertBeforeIndex = chatMessages.findIndex(el => {
        const ts = el.querySelector('.hidden-timestamp');
        return ts && new Date(ts.textContent) > new Date(tsString);
      });

      // Insert in correct chronological position
      if (insertBeforeIndex === -1) {
        chatBox.appendChild(messageElement);
      } else {
        chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
      }

console.log("-----------------28")

      //chatBox.appendChild(messageElement);
  
      if(isUserRectangleClicked){
        // Hide all chat containers in the middle section of the current tab
      Array.from(bottomMiddleSection.getElementsByClassName('chat-container')).forEach(chatContainer => {
        chatContainer.style.display = 'none';
      });
    
      const chatContainers = bottomMiddleSection.querySelectorAll('.chat-container');
      chatContainers.forEach(container => {
        const userId = container.dataset.userId;
    
        if (userId === activeRectangle.dataset.userId) { 
            container.style.display = 'block'; // Show the current user's chat container based on the latest message
        }
      });

    }

   

console.log("-----------------29")



  
   
 
   }
   

   
    
    let userLocationBox = locations_automatic[message.user_id];
    if (!userLocationBox) {
      userLocationBox = document.createElement('div');
      userLocationBox.className = 'location-box';
      userLocationBox.dataset.userId = message.user_id;
      userLocationBox.style.overflowY = 'auto';
      userLocationBox.style.padding = '10px';
      userLocationBox.style.marginBottom = '5px';

      // Define translations
      const translation_location = {
        userID: language === 'hu' ? 'Ügyfélazonosító' : 'User-ID',
        location: language === 'hu' ? 'Hely' : 'Location',
        longitude: language === 'hu' ? 'Hosszúság' : 'Longitude',
        latitude: language === 'hu' ? 'Szélesség' : 'Latitude'
        };
     
console.log("-----------------30")

      userLocationBox.innerHTML = `

        <div style="margin-bottom: 10px;">
          <div class="user-id-header">
            <i class="fa-solid fa-user" style="margin-right: 5px;"></i>
            <span class="userID_text">${translation_location.userID}</span>
          </div>
          <div class="paddingleft" style="margin-top: 5px; font-size: clamp(7px, 0.8vw, 12px);">
            <span class="full-user-id-locbox">${message.user_id !== null ? message.user_id : 'No Data'}</span>
            <span class="truncated-user-id-locbox">${message.user_id !== null ? message.user_id.substring(0, 8) : 'No Data'}</span>
          </div>
        </div>

        <div style="margin-bottom: 10px;">
          <div style="background-color: #ebeded; padding: 10px; font-weight: bold; font-size: clamp(8px, 1.2vw, 15px); border-radius: 4px;">
            <i class="fa-solid fa-location-dot" style="margin-right: 5px;"></i>
            <span class="location_userlocationbox">${translation_location.location}</span>
          </div>
          <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(7px, 0.8vw, 12px);">
            ${message.location ?? 'No Data'}
          </div>
        </div>

        <div id="location-map" style="width: 100%; height: 200px; margin-top: 10px; border-radius: 4px;"></div>



        <div style="margin-bottom: 10px;">
          <div style="background-color: #ebeded; padding: 10px; font-weight: bold; font-size: clamp(5px, 1.2vw, 15px); border-radius: 4px;">
            <i class="fa-solid fa-arrows-left-right" style="margin-right: 5px;"></i>
            <span class="longitude_userlocationbox">${translation_location.longitude}</span>
          </div>
          <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
            ${message.longitude ?? 'No Data'}
          </div>
        </div>

        <div style="margin-bottom: 10px;">
          <div style="background-color: #ebeded; padding: 10px; font-weight: bold; font-size: clamp(5px, 1.2vw, 15px); border-radius: 4px;">
            <i class="fa-solid fa-arrows-up-down" style="margin-right: 5px;"></i>  
            <span class="latitude_userlocationbox">${translation_location.latitude}</span>
          </div>
          <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
            ${message.latitude ?? 'No Data'}
          </div>
        </div>
        
      `;
      bottomRightSection.appendChild(userLocationBox);
console.log("-----------------32")
      // Initialize the map immediately after appending the content
      const mapContainer = userLocationBox.querySelector("#location-map");

      if (Number.isFinite(message.latitude) && Number.isFinite(message.longitude)) {
      

        const map = L.map(mapContainer).setView([message.latitude, message.longitude], 13);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
          attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);

        L.marker([message.latitude, message.longitude]).addTo(map)
          .bindPopup("Location: " + message.location)
          .openPopup();

        // Ensure Leaflet resizes properly
        setTimeout(() => {
          map.invalidateSize();
          }, 300);

          requestAnimationFrame(() => {
              map.invalidateSize();
          });

          // Observe element visibility and refresh map when shown
          const visibilityObserver = new IntersectionObserver(entries => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    map.invalidateSize();
                }
            });
        }, { threshold: 0.1 });
console.log("-----------------33")
        visibilityObserver.observe(userLocationBox);

        // Handle window resize events
        L.DomEvent.on(window, 'resize', () => map.invalidateSize());
      } else {
          // Hide the map container if no valid coordinates exist
          userLocationBox.querySelector("#location-map").style.display = "none";
      }

      locations_automatic[message.user_id] = userLocationBox; // Store the location box in the automatic mode object
    }
console.log("-----------------34")
    
    //HANDLING THE LOCATION AND BUTTONS TOGETHER
    if (!isUserRectangleClicked) {
      resetLocationBoxVisibility(bottomRightSection, message)
      resetButtonContainerVisibility(topMiddleSection, message)
  }else{

    const allLocationBoxes = bottomRightSection.querySelectorAll('.location-box');
    allLocationBoxes.forEach(box => {
      box.style.display = 'none'; // Hide all location boxes
    });
    allLocationBoxes.forEach(box => {
      const userId = box.dataset.userId;
      if (userId === activeRectangle.dataset.userId) {
        box.style.display = 'block'; // Show the current user's chat container based on the latest message
      }
  });

console.log("-----------------35")
    const allButtonContainers = topMiddleSection.querySelectorAll('.button-container');
    allButtonContainers.forEach(box => {
    box.style.display = 'none'; // Hide all location boxes
    });
    // Show the current user's location box
    allButtonContainers.forEach(box => {
      const userId = box.dataset.userId;
      if (userId === activeRectangle.dataset.userId) {
        box.style.display = 'flex'; // Show the current user's chat container based on the latest message
      }
    });


  }
  // showElementsForUser(message.user_id, buttonContainers);
  // const buttonContainers = topMiddleSection.querySelectorAll('.button-container');
    console.log("-----------------36")
  }
  await new Promise(requestAnimationFrame);
  chatBox.scrollTop = chatBox.scrollHeight;
  
}
 





///////////////////////////////////////////////////////////////
///////////////////////   END OF APPEND //////////////////////
/////////////////////////////////////////////////////////////







function checkAwaitingResponse(tabIndex) {
  // Access the `userRectangle` elements for the given tab
  const rectangles = Array.from(rectangle[tabIndex]?.children || []);
  // Check if any rectangle has the `awaiting-response` status
  const hasAwaitingResponse = rectangles.some(rectangle => {
      return rectangle.classList.contains('awaiting-response');
  });
  // Find the red exclamation mark element (modify selector as needed)
  const exclamationMark = document.querySelector(`[data-tab-id="${tabIndex}"] .exclamation-mark`);
  if (exclamationMark) {
      if (hasAwaitingResponse) {
          // Ensure the exclamation mark is visible
          exclamationMark.style.display = 'block';
      } else {
          // Remove the exclamation mark if no awaiting-response exists
          exclamationMark.style.display = 'none';
      }
  }
}


function showUserChatBox_Automatic(messageuserId, bottomMiddleSection) {
  
  // Hide all chat containers in the middle section of the current tab
  Array.from(bottomMiddleSection.getElementsByClassName('chat-container')).forEach(chatContainer => {
    chatContainer.style.display = 'none';
  });

  const chatContainers = bottomMiddleSection.querySelectorAll('.chat-container');
  chatContainers.forEach(container => {
    const userId = container.dataset.userId;

    if (userId === messageuserId) {
        container.style.display = 'block'; // Show the current user's chat container based on the latest message
    }
  });
}

function showButton_Automatic(userId_, topMiddleSection){
  const allButtonContainers = topMiddleSection.querySelectorAll('.button-container');
  allButtonContainers.forEach(box => {
  box.style.display = 'none'; // Hide all location boxes
  });
  // Show the current user's location box
  allButtonContainers.forEach(box => {
    const userId = box.dataset.userId;
    if (userId === userId_) {
      box.style.display = 'flex'; // Show the current user's chat container based on the latest message
    }
  });

}

function resetLocationBoxVisibility(bottomRightSection, message){
  const allLocationBoxes = bottomRightSection.querySelectorAll('.location-box');
  allLocationBoxes.forEach(box => {
    box.style.display = 'none'; // Hide all location boxes
  });
  // Show the current user's location box
  allLocationBoxes.forEach(box => {
    const userId = box.dataset.userId;
    if (userId === message.user_id) {
      box.style.display = 'block'; // Show the current user's chat container based on the latest message
    }
});
}

            
function showLocationBox_Automatic(userId, bottomRightSection) {
 
  // Hide all location boxes in the bottom right section of the current tab
  Array.from(bottomRightSection.getElementsByClassName('location-box')).forEach(locationBox => {
    locationBox.style.display = 'none';
  });
  
  // Show the selected user's location box in the current tab
  const userLocationBox = bottomRightSection.querySelector(`.location-box[data-user-id="${userId}"]`);
  if (userLocationBox) {
    userLocationBox.style.display = 'block';
  }
}

function resetChatVisibility() {
  chatContainers.forEach(container => {
      container.style.display = 'none'; // Hide all chat containers initially
  });

  // Assuming `message.user_id` contains the latest message's user ID
  chatContainers.forEach(container => {
      const userId = container.dataset.userId;

      if (userId === message.user_id) {
          container.style.display = 'block'; // Show the current user's chat container based on the latest message
      }
  });
}

function showLocationBox(userId, tabIndex) {
  
  const bottomRightSection = locations[tabIndex]; // Get the correct bottom right section for the active tab

  // Hide all location boxes in the bottom right section of the current tab
  Array.from(bottomRightSection.getElementsByClassName('location-box')).forEach(locationBox => {
    locationBox.style.display = 'none';
  });

  // Show the selected user's location box in the current tab
  const userLocationBox = bottomRightSection.querySelector(`.location-box[data-user-id="${userId}"]`);
  if (userLocationBox) {
    userLocationBox.style.display = 'block';
  }
}
  
function resetButtonContainerVisibility(topMiddleSection, message){
    const allButtonContainers = topMiddleSection.querySelectorAll('.button-container');
        allButtonContainers.forEach(box => {
        box.style.display = 'none'; // Hide all location boxes
      });
      // Show the current user's location box
      allButtonContainers.forEach(box => {
        const userId = box.dataset.userId;
        if (userId === message.user_id) {
          box.style.display = 'flex'; // Show the current user's chat container based on the latest message
        }
    });


  }
  const pastelColors = [
    '#FF6F61', // Pastel Red
    '#FFB74D', // Pastel Orange
    '#FFEB3B', // Pastel Yellow
    '#86DB6B', // Pastel Green
    '#A2DFF7', // Pastel Blue
    '#D7B2E3', // Pastel Purple
    '#F8BBD0', // Pastel Pink
    '#BFAE91', // Pastel Brown
    '#D3D3D3', // Pastel Gray
    '#F5F5F0', // Dirty White
    '#F5F5DC', // Beige
    '#FA8072', // Salmon Color
    '#EAB8E4'  // Pastel Lilac
];

  let userIndex = 0; // To keep track of the incoming user count

  // Function to get a pastel color
  function getPastelColor() {
      const color = pastelColors[userIndex % pastelColors.length];
      userIndex++; // Increment index for next call
      return color;
}
  
 
  
  // Function to get the next pastel blue color in the sequence
function getRandomPastelColor() {
    const color = pastelBlues[colorIndex]; // Get the color at the current index
    colorIndex = (colorIndex + 1) % pastelBlues.length; // Move to the next color, and wrap around if needed
    return color;
  }
function getUserColor(userId) {
    // Return a specific color for all users or certain users
    return 'linear-gradient(to right, rgb(40, 43, 77), rgb(51, 62, 104))';
  }


function showUserChatBox(userId, tabIndex) {
  // Get the middle section for the current tab
  const bottomMiddleSection = colleaguesChats[tabIndex]; // Get the correct middle section for the active tab

  // Hide all chat containers in the middle section of the current tab
  Array.from(bottomMiddleSection.getElementsByClassName('chat-container')).forEach(chatContainer => {
    chatContainer.style.display = 'none';
  });

  // Show the selected user's chat container in the current tab
  const userChatContainer = bottomMiddleSection.querySelector(`.chat-container[data-user-id="${userId}"]`);
  if (userChatContainer) {
    userChatContainer.style.display = 'block';
  }
}


function removeAdminResponseControls(userId) {
  const chatContainer = document.querySelector(`.chat-container[data-user-id="${userId}"]`);
  if (chatContainer) {
    const adminResponseControls = chatContainer.querySelector('.admin-response-controls');
    if (adminResponseControls) {
      chatContainer.removeChild(adminResponseControls);
    }
    // Get the chatBox within the chatContainer and reset its height
    const chatBox = chatContainer.querySelector('.chat-box_automatic');
    if (chatBox) {
      chatBox.style.height = 'calc(100% - 5px)'; // Revert back to original height
    }
  }
}


function switchToManualModeforOneUser(language) {    // For The original rectangle initiated the manual / automatic response
  console.log("1")
  let userId=0
  if (activeRectangle){
    userId=activeRectangle.dataset.userId
  }else{
    userId=prependeduserId
  }
  
  const chatContainer = document.querySelector(`.chat-container[data-user-id="${userId}"]`);
    // Get the chatBox within the chatContainer
  const chatBox = chatContainer ? chatContainer.querySelector('.chat-box_automatic') : null;
  if (chatBox) {
    // Adjust the height to account for the admin response controls
    chatBox.style.height = 'calc(100% - 60px)';
  }


  // Create the admin response controls to be fixed at the bottom
  const adminResponseControls = document.createElement('div');
  adminResponseControls.className = 'admin-response-controls';

  // Check selected language and set appropriate text
  const sendText = language === 'hu' ? 'Küldés' : 'Send Response';
  const placeholderText = language === 'hu' ? 'Írd ide az üzeneted...' : 'Type your response...';

  adminResponseControls.innerHTML = `
    <textarea class="manual-response" placeholder="${placeholderText}"></textarea>
    <button class="send-response">${sendText}</button>
  `;
  adminResponseControls.style.display = 'flex';
  adminResponseControls.style.alignItems = 'flex-start';
  if (!chatContainer.querySelector('.admin-response-controls')) {
  chatContainer.appendChild(adminResponseControls);
}
  // Add click event listener to send the response
  const responseInput = adminResponseControls.querySelector('.manual-response');
  const sendButton = adminResponseControls.querySelector('.send-response');

  // Set the initial height and style of the textarea
  responseInput.style.overflowY = 'hidden'; // Hide vertical scrollbar
  responseInput.style.resize = 'none'; // Prevent manual resizing
  responseInput.style.height = '30px'; // Set an initial height (min-height)

  // Auto-expand the textarea only when it exceeds the initial height
  responseInput.addEventListener('input', function () {
    this.style.height = '30px'; // Reset the height to the initial value
    if (this.scrollHeight > this.clientHeight) {
      this.style.height = (this.scrollHeight) + 'px'; // Expand if the content overflows
    }
  });

  
  async function sendResponse() {
    const response = responseInput.value;
    if (response) {
      const timestamp = new Date().toISOString();
       try {
          socket.emit('admin_response', { user_id: userId, response: response, timestamp: timestamp });
          console.log("RESP !!!!!!!!!!!!!!!!!!!!!!!: ", response)
          await handleAdminResponse(chatBox, response, null, timestamp);
       } catch (error) {
          console.error("Error in handleAdminResponse:", error);
       }
       responseInput.value = '';
       responseInput.style.height = '30px';
       
    }
 }

  sendButton.addEventListener('click', sendResponse);

  // Add 'Enter' key event listener for input box
  responseInput.addEventListener('keypress', function (event) {
    if (event.key === 'Enter') {
      event.preventDefault();
      sendResponse();
    }
  });
}

  function createChatBox_automatic(userId, tabContent) {
    // Create the container for the chatbox and response controls
    const chatContainer = document.createElement('div');
    chatContainer.className = 'chat-container';
    chatContainer.dataset.userId = userId; // Add this line to assign data-user-id
    
    // Create the chatBox for displaying messages
    const chatBox = document.createElement('div');
    chatBox.className = 'chat-box_automatic not-awaiting-response';
    chatBox.dataset.sessionId = userId;
    chatContainer.appendChild(chatBox);

    // Append the entire chat container to the tab content
    tabContent.appendChild(chatContainer);

    return chatContainer; // Return the entire container, not just chatBox
  }


  function createChatBox(userId, tabContent, tabIndex) {

    const language = localStorage.getItem("language") || "hu"; // Default to Hungarian
    const sendText = language === "hu" ? "Küldés" : "Send Response";
    const placeholderText = language === "hu" ? "Írd ide az üzeneted..." : "Type your response...";


    // Create the container for the chatbox and response controls
    const chatContainer = document.createElement('div');
    chatContainer.className = 'chat-container';
    chatContainer.dataset.userId = userId; // Add this line to assign data-user-id
    
    // Create the chatBox for displaying messages
    const chatBox = document.createElement('div');
    chatBox.className = 'chat-box not-awaiting-response';
    chatBox.dataset.sessionId = userId;
    chatContainer.appendChild(chatBox);
    
    // Create the admin response controls to be fixed at the bottom
    const adminResponseControls = document.createElement('div');
    adminResponseControls.className = 'admin-response-controls';
    adminResponseControls.innerHTML = `
      <textarea class="manual-response" placeholder="${placeholderText}" ></textarea>
      <button class="send-response" >${sendText}</button>


    `;
    adminResponseControls.style.display = 'flex';
    adminResponseControls.style.alignItems = 'flex-start';
    chatContainer.appendChild(adminResponseControls);

    
    // Add click event listener to send the response
    const responseInput = adminResponseControls.querySelector('.manual-response');
    const sendButton = adminResponseControls.querySelector('.send-response');

    // Set the initial height and style of the textarea
    responseInput.style.overflowY = 'hidden'; // Hide vertical scrollbar
    responseInput.style.resize = 'none'; // Prevent manual resizing
    responseInput.style.height = '30px'; // Set an initial height (min-height)

    // Auto-expand the textarea only when it exceeds the initial height
    responseInput.addEventListener('input', function () {
      this.style.height = '30px'; // Reset the height to the initial value
      if (this.scrollHeight > this.clientHeight) {
        this.style.height = (this.scrollHeight) + 'px'; // Expand if the content overflows
      }
    });
    
    async function sendResponse() {
      const response = responseInput.value;
      if (response) {
        const timestamp = new Date().toISOString();
        try {
       
          socket.emit('admin_response_manual_for_logging', { user_id: userId, response: response, tabIndex: tabIndex, timestamp: timestamp  });
          await handleAdminResponse(chatBox, response, counterForManualModeAddMessage[tabIndex][userId], timestamp);
          responseInput.value = '';
          responseInput.style.height = '30px';
        } catch (error) {
          console.error("Error in handleAdminResponse:", error);
       }
      }
    }

  
    sendButton.addEventListener('click', sendResponse);

    // Add 'Enter' key event listener for input box
    responseInput.addEventListener('keypress', function (event) {
      if (event.key === 'Enter') {
        sendResponse();
      }
    });

  
    // Append the entire chat container to the tab content
    tabContent.appendChild(chatContainer);
  
    return chatContainer; // Return the entire container, not just chatBox
  }
  
  
 
  

async function handleAdminResponse(chatBox, response, optionalParam = null, timestamp=null) {
    
    // const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
    //     const botResponseElement = el.querySelector('.bot-response');
    //     const adminResponseElement = el.querySelector('.admin-response');
    //     return adminResponseElement && 
    //           (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
    //             adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'));
    // });

    const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
      const botResponseElement = el.querySelector('.bot-response');
      const adminResponseElement = el.querySelector('.admin-response');
    
      // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
      return (botResponseElement && 
        (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
         botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
       (adminResponseElement && 
        (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
         adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
    });

    if (awaitingMessageElement || counterForAddAdminMessage[chatBox.dataset.sessionId]>0 || optionalParam>0) {
      let userMessage = "";
      const ts = timestamp || new Date().toISOString();
      if (awaitingMessageElement){
        const userInputElement = awaitingMessageElement.querySelector('.user-input');
        if (userInputElement) {
          userMessage = userInputElement.textContent.replace('User: ', '').replace('Ügyfél: ', '');
       } else {
          console.error("'.user-input' element not found inside awaitingMessageElement.");
       }
      }
      // if (awaitingMessageElement){
      //   const timestampVar=awaitingMessageElement.querySelector('.timestamp');
      //   if(timestampVar){
      //     timestamp = timestampVar.textContent
      //       .replace('Sent at: ', '')
      //       .replace('A küldés ideje: ', '');
      //   }else{
      //     console.error("'.timestamp' element not found inside awaitingMessageElement.");
      // }
      // }
        const sessionId = chatBox.dataset.sessionId;
        
        const latestMessage = {
            timestamp: ts,
            user_message: userMessage,
            admin_response: response,
            user_id: sessionId 
        };
        console.log("LATESTMESSAGE")
        console.log(latestMessage)
        console.log("timestamp")
        console.log(timestamp)
        console.log("awaitingMessageElement")
        console.log(awaitingMessageElement)
        console.log("counterForAddAdminMessage[chatBox.dataset.sessionId]>0")
        let a=counterForAddAdminMessage[chatBox.dataset.sessionId]>0
        console.log(a)
       
        appendMessage(latestMessage);
    

        // Send the response to the server via WebSocket
        const message = {
          response: response,
          user_id: sessionId
        };
  
        // Send the message over the WebSocket
        socket.emit('admin_response_to_the_chatbot', message);

        // This was the previous post request
        // fetch('http://127.0.0.1:8000/sent_admin_response', {
        //     method: 'POST',
        //     headers: {
        //         'Content-Type': 'application/json'
        //     },
        //     body: JSON.stringify({ response: response, user_id: sessionId })
        // })
        //     .then(response => response.json())
        //     .then(data => console.log('Response sent successfully:', data))
        //     .catch(error => console.error('Error sending response:', error));
    } else {
        console.error('No message found with "Awaiting Admin Response..."');
    }
}

function showEditTabs() {
  currentTabMode = 'edit';
 
  // Hide the initial tabs input container
  const tabsInputContainer = document.getElementById('tabs-input-container');
  const editTabsContainer = document.getElementById('edit-tabs-container');

  tabsInputContainer.style.display = 'none';
  editTabsContainer.style.display = 'block';

  socket.emit('show_edit_tabs', { 
  timestamp: new Date().toISOString()
});
}

function showTabsInput() {
  currentTabMode = 'input';
  // Show the initial tabs input container
  const tabsInputContainer = document.getElementById('tabs-input-container');
  const editTabsContainer = document.getElementById('edit-tabs-container');

  tabsInputContainer.style.display = 'block';
  editTabsContainer.style.display = 'none';

  socket.emit('show_tabs_input', { frontend_time: new Date().toISOString() });

  
}

function addColleagueTab(tabData){
    
    const [{ colleagueName, uniqueId }] = tabData;
    // const uniqueId = `tab-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const index = tabsContainer.children.length; // Get the current number of tabs as the index for the new tab
    const tab = document.createElement('div');
    tab.classList.add('tab');
    tab.classList.add('clickable');
    tab.dataset.tabId = uniqueId;
    tab.dataset.name = colleagueName;

    // Set display style to flex to align items in a row
    tab.style.display = 'flex';
    tab.style.alignItems = 'center';  // Vertically center the items


    const tabName = document.createElement('span');
    tabName.textContent = colleagueName;
    tabName.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
    tab.appendChild(tabName);

    const exclamationMark = document.createElement('span');
    exclamationMark.classList.add('exclamation-mark');
    exclamationMark.textContent = '!';
    exclamationMark.style.color = 'red';
    exclamationMark.style.display = 'none'; // Initially hidden
    exclamationMark.style.marginLeft = '8px'; // Adjust spacing
    exclamationMark.style.fontWeight = 'bold';
    exclamationMark.style.fontSize = 'clamp(7px, 0.8vw, 12px)';

    tab.appendChild(exclamationMark);
    tab.onclick = () => showTab(uniqueId);
    if (index === 0) tab.classList.add('active'); // Set the first tab as active

    tabsContainer.appendChild(tab); // Add the new tab to the tabs container

    // Create new tab content
    const content = document.createElement('div');
    content.classList.add('tab-content');
    content.dataset.tabIndex = uniqueId; // Associate content with tab index

    // Initially, hide all tab contents except the first
    content.style.display = 'none';

    // Create Grid Layout for Tab Content with 2 rows and 3 columns
    const topRow = document.createElement('div');
    topRow.classList.add('top-row');

    const language = localStorage.getItem('language') || 'hu';
    
    const topLeftSection = document.createElement('div');
    topLeftSection.classList.add('top-left-section');
    topLeftSection.textContent = translations[language]['customers'] || 'Ügyfelek';
    // topLeftSection.textContent = 'Customers';
    topLeftSection.style.fontWeight = 'bold'; 
    // topLeftSection.style.fontSize = '1.2vw';
    

    const topMiddleSection = document.createElement('div');
    topMiddleSection.classList.add('top-middle-section');
    topMiddleSection.textContent = 'Chats';
    topMiddleSection.style.fontWeight = 'bold';
    //topMiddleSection.style.fontSize = '1.2vw';
    
   

    const topRightSection = document.createElement('div');
    topRightSection.classList.add('top-right-section');
 
    topRightSection.textContent = translations[language]['customerDetails'] || 'Customer details'; // Fallback to English if translation is not found
  
    // topRightSection.textContent = 'Customer details';
    topRightSection.style.fontWeight = 'bold';
    //topRightSection.style.fontSize = '1.2vw';

    window.addEventListener('resize', () => {
      // Calculate new font size based on window width
      let newFontSize = window.innerWidth / 50; // Adjust as needed
      
      // Ensure it doesn't exceed the maximum size
      newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
      topLeftSection.style.fontSize = `${newFontSize}px`;
      topMiddleSection.style.fontSize = `${newFontSize}px`;
      topRightSection.style.fontSize = `${newFontSize}px`;
    });
    

    topRow.appendChild(topLeftSection);
    topRow.appendChild(topMiddleSection);
    topRow.appendChild(topRightSection);

    const bottomRow = document.createElement('div');
    bottomRow.classList.add('bottom-row');

    const bottomLeftSection = document.createElement('div');
    bottomLeftSection.classList.add('bottom-left-section');

    const bottomMiddleSection = document.createElement('div');
    bottomMiddleSection.classList.add('bottom-middle-section');

    const bottomRightSection = document.createElement('div');
    bottomRightSection.classList.add('bottom-right-section');

    bottomRow.appendChild(bottomLeftSection);
    bottomRow.appendChild(bottomMiddleSection);
    bottomRow.appendChild(bottomRightSection);

    content.appendChild(topRow);
    content.appendChild(bottomRow);
    tabContentsContainer.appendChild(content);
    colleaguesChats[uniqueId] = bottomMiddleSection; // Map index to the middle bottom content element where chatboxes go
    rectangle[uniqueId] = bottomLeftSection;
    locations[uniqueId]= bottomRightSection
    counterForManualModeAddMessage[uniqueId]={}

    // Clear the input field
    const addColleagueInput = document.getElementById('add-colleague');
    addColleagueInput.value = '';
    
    
}

function addColleague() {
  const addColleagueInput = document.getElementById('add-colleague');
  const colleagueName = addColleagueInput.value.trim();
  const tabsData = [];

  if (colleagueName) {
    // socket.emit('add_colleague', colleagueName);
    // Create a new tab
    const uniqueId = `tab-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   
    // Add tab data to the array
    tabsData.push({ colleagueName, uniqueId });
    // Create a tab
    const tab = document.createElement('div');
    tab.classList.add('tab');
    tab.classList.add('clickable');
    tab.dataset.tabId = uniqueId;
    tab.dataset.name = colleagueName;

    // Set display style to flex to align items in a row
    tab.style.display = 'flex';
    tab.style.alignItems = 'center';  // Vertically center the items

    // Assuming each tab has a unique ID assigned
    

    const tabName = document.createElement('span');
    tabName.textContent = colleagueName;
    tabName.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
    tab.appendChild(tabName);

    const exclamationMark = document.createElement('span');
    exclamationMark.classList.add('exclamation-mark');
    exclamationMark.textContent = '!';
    exclamationMark.style.color = 'red';
    exclamationMark.style.display = 'none'; // Initially hidden
    exclamationMark.style.marginLeft = '8px'; // Adjust spacing
    exclamationMark.style.fontWeight = 'bold';
    exclamationMark.style.fontSize = 'clamp(7px, 0.8vw, 12px)';

    tab.appendChild(exclamationMark);

    tab.onclick = () => showTab(uniqueId);
    // Enable double-click to rename the tab
    tab.ondblclick = () => {
      const currentName = tabName.textContent;

      // Create an input field to replace the tab name
      const input = document.createElement('input');
      input.type = 'text';
      input.value = String(currentName);
      input.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
      input.style.width = '100%'; // Adjust to fit tab width
      input.style.border = 'none';
      input.style.outline = 'none';
      input.style.background = 'transparent';
      input.style.color = 'inherit';
      input.style.textAlign = 'center';

      // Replace the tab name with the input field
      tab.replaceChild(input, tabName);

      // Focus and select the input field
      input.focus();
      input.select();

      // Save the new name when the input loses focus or Enter is pressed
      const saveName = () => {
        const newName = String(input.value.trim());

        // If the name is not empty, update it
        if (newName) {
          tabName.textContent = newName;

          // Update the tabsData array for this tab
          const tabData = tabsData.find(t => t.uniqueId === uniqueId);
          if (tabData) tabData.name = newName;

          socket.emit('update_tab_name', { uniqueId: uniqueId,
              newName: newName,
              frontend_time: new Date().toISOString() });
                    }

        // Replace the input field with the updated name
        tab.replaceChild(tabName, input);
      };

      // Handle saving when Enter is pressed
      input.onkeypress = (e) => {
        if (e.key === 'Enter') saveName();
      };

      // Handle saving when focus is lost
      input.onblur = saveName;
    };
    tabsContainer.appendChild(tab);

    // Create tab content
    const content = document.createElement('div');
    content.classList.add('tab-content');
    content.dataset.tabIndex = uniqueId; // Associate content with tab index

    // Initially, hide all tab contents except the first
    content.style.display = 'none';

    // Create Grid Layout for Tab Content with 2 rows and 3 columns
    const topRow = document.createElement('div');
    topRow.classList.add('top-row');
    const language = localStorage.getItem('language') || 'hu';
    const topLeftSection = document.createElement('div');
    topLeftSection.classList.add('top-left-section');
    // topLeftSection.textContent = 'Customers';
    topLeftSection.textContent = translations[language]['customers'] || 'Ügyfelek';
    topLeftSection.style.fontWeight = 'bold'; 
    // topLeftSection.style.fontSize = '1.2vw';
    

    const topMiddleSection = document.createElement('div');
    topMiddleSection.classList.add('top-middle-section');
    topMiddleSection.textContent = language === 'hu' ? 'Üzenetek' : 'Chats';
    topMiddleSection.style.fontWeight = 'bold';
    //topMiddleSection.style.fontSize = '1.2vw';
    
   

    const topRightSection = document.createElement('div');
    topRightSection.classList.add('top-right-section');
    topRightSection.textContent = translations[language]['customerDetails'] || 'Customer details'; // Fallback to English if translation is not found
  
    // topRightSection.textContent = 'Customer details';
    topRightSection.style.fontWeight = 'bold';
    //topRightSection.style.fontSize = '1.2vw';

    window.addEventListener('resize', () => {
      // Calculate new font size based on window width
      let newFontSize = window.innerWidth / 50; // Adjust as needed
      
      // Ensure it doesn't exceed the maximum size
      newFontSize = Math.min(maxFontSize, Math.max(newFontSize, minFontSize));
      topLeftSection.style.fontSize = `${newFontSize}px`;
      topMiddleSection.style.fontSize = `${newFontSize}px`;
      topRightSection.style.fontSize = `${newFontSize}px`;
    });
    

    topRow.appendChild(topLeftSection);
    topRow.appendChild(topMiddleSection);
    topRow.appendChild(topRightSection);

    const bottomRow = document.createElement('div');
    bottomRow.classList.add('bottom-row');

    const bottomLeftSection = document.createElement('div');
    bottomLeftSection.classList.add('bottom-left-section');

    const bottomMiddleSection = document.createElement('div');
    bottomMiddleSection.classList.add('bottom-middle-section');

    const bottomRightSection = document.createElement('div');
    bottomRightSection.classList.add('bottom-right-section');

    bottomRow.appendChild(bottomLeftSection);
    bottomRow.appendChild(bottomMiddleSection);
    bottomRow.appendChild(bottomRightSection);

    content.appendChild(topRow);
    content.appendChild(bottomRow);
    tabContentsContainer.appendChild(content);
    colleaguesChats[uniqueId] = bottomMiddleSection; // Map index to the middle bottom content element where chatboxes go
    rectangle[uniqueId] = bottomLeftSection;
    locations[uniqueId]= bottomRightSection
    counterForManualModeAddMessage[uniqueId]={}


    // Clear the input field
    addColleagueInput.value = '';
  } else {
    alert('Please enter a valid colleague name.');
  }
  socket.emit('add_colleague', { tabs: tabsData, timestamp: new Date().toISOString() });
}

function extractLocationData(innerText, targetUserId) {
  // Define the regex pattern to match the structure
  const regex = /User-ID\s*(\d+)\s*Longitude\s*([\w\s]*)\s*Latitude\s*([\w\s]*)\s*Location\s*(?:\n\s*)*(.+?)(?=\n)/g;

  // An object to hold the matched data for each User-ID
  const locations = {};

  // Iterate through all matches in the text
  let match;
  while ((match = regex.exec(innerText)) !== null) {
      const userId = match[1].trim();
      const longitude = match[2].trim();
      const latitude = match[3].trim();
      const location = match[4].trim();

      // Store the data only if the userId matches the targetUserId
      if (userId === targetUserId.toString()) {
          locations[userId] = {
              longitude: longitude === 'No Data' ? null : longitude,
              latitude: latitude === 'No Data' ? null : latitude,
              location: location === 'No Data' ? null : location
          };
      }
  }

 
  // Return the data for the target user
  return locations[targetUserId] || null;
}

const exampleText = `
User-ID\n11\n Longitude\nNo ha\n Latitude\nNo rh\n Location\nNo rrr

`;


function removeColleague() {
  const removeColleagueInput = document.getElementById('remove-colleague');
  const colleagueName = removeColleagueInput.value.trim();
  if (colleagueName) {
    socket.emit('remove_colleague', { colleagueName: colleagueName, timestamp: new Date().toISOString() });
    removeColleagueInput.value = '';
    const tabs = document.querySelectorAll('.tab');
    let tabIndex = -1;
    let uniqueTabId=-1
    // Find the index of the tab with the matching colleague name
    tabs.forEach((tab, index) => {
        if (tab.dataset.name === colleagueName) {
            tabIndex = index;
            uniqueTabId=tab.dataset.tabId;
        }
    });

    if (uniqueTabId !== -1) {
        
      // Gather the chat content of the colleague being removed
      const chatsToDistribute = colleaguesChats[uniqueTabId]?.innerHTML || '';
      const locationsToDistribute = locations[uniqueTabId]?.innerHTML || '';
      // Remove the tab and its content
      const tab = document.querySelector(`.tab[data-tab-id="${uniqueTabId}"]`);
      tabsContainer.removeChild(tab);
      const contents = document.querySelectorAll('.tab-content');
      const contentToRemove = document.querySelector(`.tab-content[data-tab-index="${uniqueTabId}"]`);
      tabContentsContainer.removeChild(contentToRemove);
      // Update the data structures
      delete colleaguesChats[uniqueTabId];
      delete rectangle[uniqueTabId];
      delete locations[uniqueTabId];


       // Handle tab switch if the active tab was removed
      if (tab.classList.contains('active')) {
        // If the removed tab was the active tab, select another tab
        if (tabs.length > 1) {
          const newTabIndex = (tabIndex === 0) ? 1 : tabIndex - 1; // Go to the previous tab if possible, else next
          const newActiveTab = tabs[newTabIndex];
          showTab(newActiveTab.dataset.tabId);
        } else {
          // If this was the last tab, clear the content display
          tabContentsContainer.innerHTML = '';
        }
      }    
      if (chatsToDistribute) {
        try {
          const parser = new DOMParser();
          const doc = parser.parseFromString(chatsToDistribute, 'text/html');
          
          // Select all chat containers
          const chatContainers = doc.querySelectorAll('.chat-container');
          
          chatContainers.forEach((chatContainer) => {
            const chatBoxes = chatContainer.querySelectorAll('.chat-box'); // Select all chat-boxes inside the chatContainer
            let counter = 0
            chatBoxes.forEach((chatBox) => {
              const messages = chatBox.querySelectorAll('.message'); // Select all messages within the chatBox
              let counter = 0
              messages.forEach((message, index) => {
                counter++; 
                const messageData = {
                  timestamp: null,
                  user_id: null,
                  user_message: null,
                  admin_response: null,
                  latitude: null,
                  longitude: null,
                  location: null,
                  flag: "deleted",
                  awaiting: null,
                  name: colleagueName,
                  message_number: counter
                };
    
                // Extract data from the message element
                const userId_ = chatContainer.getAttribute('data-user-id');
                messageData.user_id = userId_;
    
                // Find the timestamp
                const timestampElement = message.querySelector('.timestamp');
                //const timestamp_ = timestampElement ? timestampElement.textContent.replace('Sent at: ', '').trim() : '';
                const timestamp_ = timestampElement ? timestampElement.textContent.replace(/^(Sent at: |A küldés ideje: )/, '').trim() : '';

                messageData.timestamp = timestamp_;
    
                // Find the user message
                const userMessageElement = message.querySelector('.user-input');
                //const userMessage_ = userMessageElement ? userMessageElement.textContent.replace('User: ', '').trim() : '';
                const userMessage_ = userMessageElement ? userMessageElement.textContent.replace(/^(User: |Ügyfél: )/, '').trim() : '';

                messageData.user_message = userMessage_;
    
                // Find the admin response
                const adminResponseElement = message.querySelector('.admin-response');
                let adminMessage_ = adminResponseElement ? adminResponseElement.textContent.trim() : '';
                // Check if the admin message starts with 'Admin:'
                // if (adminMessage_.startsWith('Admin:')) {
                //   // Remove 'Admin:' from the start of the string
                //   adminMessage_ = adminMessage_.substring(6).trim(); // Removes 'Admin:' and trims any leading space
                // }
                messageData.admin_response = adminMessage_;
                const isLastMessage = index === messages.length - 1;
                if (isLastMessage) {
                  // If it's the last message, check if chatBox has the 'awaiting-response' attribute
                  if (chatBox.classList.contains('awaiting-response')) {
                    messageData.awaiting = true;
                  } else {
                    messageData.awaiting = false;
                  }
                } else {
                  // For all other messages, set awaiting to false
                  messageData.awaiting = false;
                }



                // Process location data
                if (locationsToDistribute) {
                  const parser = new DOMParser();
                  const doc = parser.parseFromString(locationsToDistribute, 'text/html');
                  const locationBoxes = doc.querySelectorAll('.location-box');
  
                  locationBoxes.forEach((box) => {
                    const userId = box.getAttribute('data-user-id');
                    if (userId === userId_) {
                      const longitudeElement = box.querySelector('i.fa-arrows-left-right').parentElement.nextElementSibling;
                      const latitudeElement = box.querySelector('i.fa-arrows-up-down').parentElement.nextElementSibling;
                      const locationElement = box.querySelector('i.fa-location-dot').parentElement.nextElementSibling;

                      messageData.longitude = longitudeElement ? longitudeElement.textContent.trim() : 'No Data';
                      messageData.latitude = latitudeElement ? latitudeElement.textContent.trim() : 'No Data';
                      messageData.location = locationElement ? locationElement.textContent.trim() : 'No Data';
                      }
                    });
                  }
                  // Append the message data
                  appendMessage(messageData);
                });
            });
        });
          
        } catch (error) {
            console.error("Error while processing messages:", error);
        }

        

      }
   
    }
  }
}

function getTabName(tabId) {
  // Find the tab element by its data-tab-id
  const tab = document.querySelector(`[data-tab-id="${tabId}"]`);
  if (tab) {
    // Find the span element containing the tab name
    const tabName = tab.querySelector('span');
    if (tabName) {
      return tabName.textContent; // Return the text content of the tab name
    } else {
      console.error(`Tab with ID ${tabId} found, but name element is missing.`);
      return null; // Return null if the name element is missing
    }
  } else {
    console.error(`Tab with ID ${tabId} not found.`);
    return null; // Return null if the tab is not found
  }
}

function manageRectangleDragAndDrop(userId, fromTab, toTab) {
  // Locate the rectangle in the source tab
  const rectangleToMove = rectangle[fromTab]?.querySelector(`[data-user-id="${userId}"]`);
  const chatToMove = colleaguesChats[fromTab]?.querySelector(`[data-user-id="${userId}"]`);
  const locationToMove = locations[fromTab]?.querySelector(`[data-user-id="${userId}"]`);

  const fromTabContent = document.querySelector(`.tab-content[data-tab-index="${fromTab}"]`);
  const chatContainer = fromTabContent.querySelector(`.chat-container[data-user-id="${userId}"]`);
  const locationBox = fromTabContent.querySelector(`.location-box[data-user-id="${userId}"]`);

  if (!rectangleToMove || !chatToMove || !locationToMove) {
      
      console.error('Unable to find rectangle, chat, or location box for userId:', userId);
      return;
  }

  // Remove the rectangle from the source tab
  rectangle[fromTab]?.removeChild(rectangleToMove);

  // Remove the chat container from the source tab
  colleaguesChats[fromTab]?.removeChild(chatToMove);

  // Remove the location box from the source tab
  locations[fromTab]?.removeChild(locationToMove);

  
  if (chatContainer) {
    const chatBoxes = chatContainer.querySelectorAll('.chat-box'); // Select chat-boxes in the container
    
    chatBoxes.forEach((chatBox) => {
        const messages = chatBox.querySelectorAll('.message'); // Select all messages within the chatBox
        let counter = 0  
        messages.forEach((message, index) => {
            counter++;
            const messageData = {
                timestamp: null,
                user_id: userId,
                user_message: null,
                admin_response: null,
                latitude: null,
                longitude: null,
                location: null,
                flag: "deleted", // Set to "dragged" as part of the operation
                awaiting: null,
                name: getTabName(fromTab),
                message_number: counter
            };

            // Extract the timestamp
            const timestampElement = message.querySelector('.timestamp');
            //messageData.timestamp = timestampElement ? timestampElement.textContent.replace('Sent at: ', '').trim() : '';
            messageData.timestamp = timestampElement ? timestampElement.textContent.replace(/^(Sent at: |A küldés ideje: )/, '').trim() : '';


            // Extract the user message
            const userMessageElement = message.querySelector('.user-input');
            //messageData.user_message = userMessageElement ? userMessageElement.textContent.replace('User: ', '').trim() : '';
            messageData.user_message = userMessageElement ? userMessageElement.textContent.replace(/^(User: |Ügyfél: )/, '').trim() : '';


            // Extract the admin response
            const adminResponseElement = message.querySelector('.admin-response');
            messageData.admin_response = adminResponseElement ? adminResponseElement.textContent.trim() : '';

            // Determine if this is the last message and its awaiting status
            const isLastMessage = index === messages.length - 1;
            if (isLastMessage) {
                messageData.awaiting = chatBox.classList.contains('awaiting-response');
            } else {
                messageData.awaiting = false;
            }

            // Extract location data if available
            
            if (locationBox) {
                const longitudeElement = locationBox.querySelector('i.fa-arrows-left-right').parentElement.nextElementSibling;
                const latitudeElement = locationBox.querySelector('i.fa-arrows-up-down').parentElement.nextElementSibling;
                const locationElement = locationBox.querySelector('i.fa-location-dot').parentElement.nextElementSibling;

                messageData.longitude = longitudeElement ? longitudeElement.textContent.trim() : 'No Data';
                messageData.latitude = latitudeElement ? latitudeElement.textContent.trim() : 'No Data';
                messageData.location = locationElement ? locationElement.textContent.trim() : 'No Data';
            }
            // Process or append the messageData as needed
            //appendMessageSavedStates(messageData, toTab);
            
            appendMessageSavedStates(messageData, toTab, fromTab);
        });
    });

    
  }

 
}

function cleanObjectsButKeepZero() {   // clear the conent expect the core 0 elements, in order that mode change you can display earlier conversations
  for (const key in Chats_automatic) {
    if (key !== "0") {
      delete Chats_automatic[key];
    }
  }

  for (const key in rectangle_automatic) {
    if (key !== "0") {
      delete rectangle_automatic[key];
    }
  }

  for (const key in locations_automatic) {
    if (key !== "0") {
      delete locations_automatic[key];
    }
  }
  // Clear all keys from automaticResponseStates
  for (const key in automaticResponseStates) {
    delete automaticResponseStates[key];
  }

  for (const key in userElements) {
    delete userElements[key];
  }
}


function handleModeSwitch() {
    manualMode = !manualMode;
    const language = localStorage.getItem('language') || 'hu';
    toggleButton.textContent = manualMode ? translations[language]['toggleModeManual'] : translations[language]['toggleMode'];
    
    // toggleButton.textContent = manualMode ? 'Switch to Automatic Response' : 'Switch to Manual Response';
    // Update button style
    toggleButton.style.cursor = 'pointer';
    toggleButton.style.backgroundColor = manualMode ? '#007BFF' : '#28A745'; // Toggle between two colors
    toggleButton.style.color = '#FFFFFF';
    toggleButton.style.width = '120px';  // Set width as defined
    toggleButton.style.height = '40px';  // Set height as defined
    toggleButton.style.border = 'none';  // Remove border
    toggleButton.style.borderRadius = '30px';  // Set rounded corners to 30px
    toggleButton.style.fontSize = '10px';  // Set font size to 10px
    toggleButton.style.marginRight = '20px'; 
    

    const tabsInputContainer = document.getElementById('tabs-input-container');
    const label = document.querySelector('label[for="colleagues"]');
    const labelAddC = document.querySelector('label[for="add-colleague"]');
    const labelremoveC = document.querySelector('label[for="remove-colleague"]');
    const input = document.getElementById('colleagues');
    const inputaddC = document.getElementById('add-colleague');
    const inputremoveC = document.getElementById('remove-colleague');
    const createTabsButton = document.getElementById('create-tabs-button');
    const tabContainer = document.querySelector('.tab-container')

    if (manualMode) {
  
      // Show the container and apply styles
      tabsInputContainer.classList.add('visible');
      tabsInputContainer.style.display = 'flex';
      tabsInputContainer.style.padding = '0.9%';
      tabsInputContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
      tabsInputContainer.style.borderRadius = '10px';
      tabsInputContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
      tabsInputContainer.style.marginBottom = '10px';
      tabContainer.style.height = '70vh';


      // Apply styles to the label
      label.style.color = 'white';
      
      label.style.marginRight = '20px';

      // Apply styles to the input
      input.style.padding = '3px';
      input.style.border = 'none';
      input.style.borderRadius = '10px';
      input.style.flexGrow = '1';
      input.style.marginRight = '20px';
      input.style.fontSize = '13px';

     

      // Apply styles to the button
      createTabsButton.style.padding = '4px 15px'; // Button padding
      createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
      createTabsButton.style.color = 'white'; // Button text color
      createTabsButton.style.border = 'none'; // No border
      createTabsButton.style.borderRadius = '30px'; // Rounded corners
      createTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
      createTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
      
      // Add hover effect
      createTabsButton.onmouseover = function() {
          createTabsButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
      };

      createTabsButton.onmouseout = function() {
          createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
      };

      // Apply styles to the button
      editTabsButton.style.padding = '4px 15px'; // Button padding
      editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
      editTabsButton.style.color = 'white'; // Button text color
      editTabsButton.style.border = 'none'; // No border
      editTabsButton.style.borderRadius = '30px'; // Rounded corners
      editTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
      editTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
      editTabsButton.style.marginLeft = '20px';
      
      // Add hover effect
      editTabsButton.onmouseover = function() {
        editTabsButton.style.backgroundColor = '#b02a37'; // Darker green on hover
      };

      editTabsButton.onmouseout = function() {
          editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
      };
      

      function updateFontSize() {
        // Ensure this targets the correct label
     
       // Get the viewport width
       const viewportWidth = window.innerWidth;
     
       // Calculate the font size (using a ratio of the viewport width)
       const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width
     
       // Define minimum and maximum font size
       const minFontSize = 10; // Minimum font size (in px)
       const maxFontSize = 14; // Maximum font size (in px)
     
       // Apply the responsive font size with the min and max limits
       const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);
     
       // Set the calculated font size to the label
       label.style.fontSize = `${fontSize}px`;
       input.style.fontSize = `${fontSize}px`;
       editTabsButton.style.fontSize=`${fontSize}px`;
       createTabsButton.style.fontSize=`${fontSize}px`;
     }
     
     // Apply the function on initial load
     updateFontSize();
     
     // Update the font size whenever the window is resized
     window.addEventListener('resize', updateFontSize);
      
  } else {
 
      // Hide the container when in automatic mode
      tabsInputContainer.classList.remove('visible');
      tabsInputContainer.style.display = 'none';;
      editTabsContainer.style.display = 'none';
      tabContainer.style.height = '80vh';
      
      // Call the function
      cleanObjectsButKeepZero();
      createDefaultTabForAutomaticMode()
    
  }
  editTabsButton.addEventListener('click', function() {
    // Toggle the visibility of the edit tabs container
        editTabsContainer.classList.add('visible');
        
        // Style the editTabsContainer similarly to tabsInputContainer
        editTabsContainer.style.padding = '0.9%';
        editTabsContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
        editTabsContainer.style.borderRadius = '10px';
        editTabsContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
        editTabsContainer.style.marginBottom = '10px';

     
        addColleagueButton.style.padding = '4px 15px'; // Button padding
        addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
        addColleagueButton.style.color = 'white'; // Button text color
        addColleagueButton.style.border = 'none'; // No border
        addColleagueButton.style.borderRadius = '30px'; // Rounded corners
        addColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
        addColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
        addColleagueButton.style.marginRight = '20px';
        addColleagueButton.style.height='29px';
         // Add hover effect
         addColleagueButton.onmouseover = function() {
          addColleagueButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
      };

      addColleagueButton.onmouseout = function() {
        addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
      };

      
        backButton.style.padding = '4px 15px'; // Button padding
        backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // 
        backButton.style.color = 'white'; // Button text color
        backButton.style.border = 'none'; // No border
        backButton.style.borderRadius = '30px'; // Rounded corners
        backButton.style.cursor = 'pointer'; // Pointer cursor on hover
        backButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
        backButton.style.height='29px';
         // Add hover effect
         backButton.onmouseover = function() {
          backButton.style.backgroundColor = '#157f4f'; // Darker green on hover
        };

        backButton.onmouseout = function() {
          backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // Reset to original color
        };

        
        removeColleagueButton.style.padding = '4px 15px'; // Button padding
        removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
        removeColleagueButton.style.color = 'white'; // Button text color
        removeColleagueButton.style.border = 'none'; // No border
        removeColleagueButton.style.borderRadius = '30px'; // Rounded corners
        removeColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
        removeColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
        removeColleagueButton.style.marginRight = '30px';
        removeColleagueButton.style.height = '29px';

         // Add hover effect
         removeColleagueButton.onmouseover = function() {
          removeColleagueButton.style.backgroundColor = '#b02a37'; // Darker green on hover
        };

        removeColleagueButton.onmouseout = function() {
          removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
        };
        
        // Apply styles to the label
        labelAddC.style.color = 'white';
        labelAddC.style.fontSize = '14px';
        labelAddC.style.marginRight = '2px';
        labelAddC.style.marginTop = '3px';

        // Apply styles to the label
        labelremoveC.style.color = 'white';
        labelremoveC.style.fontSize = '14px';
        labelremoveC.style.marginRight = '2px';
        labelremoveC.style.marginTop = '3px';

        inputaddC.style.padding = '3px';
        inputaddC.style.height = '27px';
        inputaddC.style.border = 'none';
        inputaddC.style.borderRadius = '10px';
        inputaddC.style.flexGrow = '1';
        inputaddC.style.marginLeft = '20px';
        inputaddC.style.marginRight = '20px';
        inputaddC.style.fontSize = '13px';
        inputaddC.style.minWidth = '40px';
        inputaddC.style.boxSizing = 'border-box';

        inputremoveC.style.padding = '3px';
      
        inputremoveC.style.border = 'none';
        inputremoveC.style.borderRadius = '10px';
        inputremoveC.style.flexGrow = '1';
        inputremoveC.style.marginRight = '20px';
        inputremoveC.style.marginLeft = '20px';
        inputremoveC.style.fontSize = '13px';
        inputremoveC.style.minWidth = '40px';
        inputremoveC.style.boxSizing = 'border-box';
        inputremoveC.style.height = '27px'; 

       

        function updateFontSize2() {
          // Ensure this targets the correct label
       
         // Get the viewport width
         const viewportWidth = window.innerWidth;
       
         // Calculate the font size (using a ratio of the viewport width)
         const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width
       
         // Define minimum and maximum font size
         const minFontSize = 10; // Minimum font size (in px)
         const maxFontSize = 14; // Maximum font size (in px)
       
         // Apply the responsive font size with the min and max limits
         const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);
       
         // Set the calculated font size to the label
         labelremoveC.style.fontSize = `${fontSize}px`;
         inputremoveC.style.fontSize = `${fontSize}px`;
         labelAddC.style.fontSize = `${fontSize}px`;
         inputaddC.style.fontSize = `${fontSize}px`;
         addColleagueButton.style.fontSize=`${fontSize}px`;
         backButton.style.fontSize=`${fontSize}px`;
         removeColleagueButton.style.fontSize=`${fontSize}px`;

       
       }
       
       // Apply the function on initial load
       updateFontSize2();
       
       // Update the font size whenever the window is resized
       window.addEventListener('resize', updateFontSize2);
      
      
    });
 
    // const payload = {
    //     mode: manualMode ? 'manual' : 'automatic',
    //     frontend_time: new Date().toISOString() // UTC ISO string
    // };

    // socket.emit('mode_changed', payload);

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        const payload = {
          mode: manualMode ? 'manual' : 'automatic',
          frontend_time: new Date().toISOString()
        };
        socket.emit('mode_changed', payload);
      });
    });

   
        
}

// function startEventSource() {
//   eventSource = new EventSource('/stream_chat');
//   eventSource.onopen = function () {
//     console.log("Connection to /stream_chat opened");
// };
//   eventSource.onmessage = function (event) {
//       try {
//           const message = JSON.parse(event.data);
//           appendMessage(message);
//       } catch (error) {
//           console.error('Error parsing message:', error);
//       }
//   };

//   eventSource.onerror = function (event) {
//       console.error('SSE Error:', event);
//   };
  

//   // Ensure the connection is established after the page has fully loaded

//   // Initialize automatic mode setup on page load
//   if (!manualMode) {
//       console.log("Initializing automatic mode on page load...");
//       tabsContainer.innerHTML = '<div class="active">AI Response</div>';
//       tabContentsContainer.innerHTML = '<div class="tab-content active"></div>';
//       colleaguesChats[0] = tabContentsContainer.querySelector('.tab-content');
//   } else {
//       // In manual mode, ensure the latest user's chat box is shown on load
//       const bottomLeftSection = document.querySelector('.bottom-left-section');
//       const latestUserRectangle = Array.from(bottomLeftSection.querySelectorAll('.user-rectangle'))
//           .sort((a, b) => new Date(b.dataset.timestamp) - new Date(a.dataset.timestamp))[0];
//       if (latestUserRectangle) {
//           showUserChatBox(latestUserRectangle.dataset.userId);
//       }
//   }
// }

// Toggle button event
toggleButton.addEventListener('click', handleModeSwitch);


function adjustTabWidths() {
    const tabsContainer = document.querySelector('.tabs');
    const tabs = Array.from(tabsContainer.children);
    
    // Get the width of the container
    const containerWidth = tabsContainer.clientWidth;
    
    // Calculate the number of tabs per row based on the available space
    const minTabWidth = 150; // Set a smaller minimum width to allow for shrinking
    const gap = 5; // The gap between tabs
    const tabsPerRow = Math.floor((containerWidth + gap) / (minTabWidth + gap));

    // Calculate the new width for each tab so all tabs in a row have equal width
    const newTabWidth = (containerWidth - (tabsPerRow - 1) * gap) / tabsPerRow;
    
    // Set the width for each tab and make sure they shrink if necessary
    tabs.forEach(tab => {
        tab.style.width = `${newTabWidth}px`;
    });
}

// Call the function initially to set up the widths
adjustTabWidths();

// Adjust widths when the window is resized
window.addEventListener('resize', adjustTabWidths);

input.addEventListener('input', () => {
  const inputValue = String(input.value);

  const payload = {
    value: inputValue,
    timestamp: new Date().toISOString() // always UTC ISO string
  };

  socket.emit('update_colleagues_input', payload);

});






inputAddOneColleague.addEventListener('input', () => {
  const inputValueAddOne = inputAddOneColleague.value;
  const timestamp = new Date().toISOString();
  socket.emit('one_colleague_input', {inputValueAddOne, timestamp});
});

removeOneColleague.addEventListener('input', () => {
  const inputValueRemoveOne = removeOneColleague.value;
  const timestamp = new Date().toISOString();
  socket.emit('remove_onecolleague_input', { inputValueRemoveOne: inputValueRemoveOne, timestamp: timestamp });
});






    //////////////////////////////////////////////////////                  ////////////////////////////////////////////////////
    //        SOCKET  Additionla FUNCTIONS   ////////////                   //      SOCKET Additional FUNCTION      ////////////
    /////////////////////////////////////////////////////  
    ////////////////////////////////////////////////////

    

function updateTabName(uniqueId, newName) {
  const tab = document.querySelector(`[data-tab-id="${uniqueId}"]`);
  if (tab) {
    const tabNameElement = tab.querySelector('span'); // Assuming the first span holds the tab name
    if (tabNameElement) {
      tabNameElement.textContent = newName; // Update the displayed name
    }
  }
}


function removeColleagueforSocket(removeColleagueInput) {
  const colleagueName = removeColleagueInput.colleague_name.trim();
  if (colleagueName) {
    // socket.emit('remove_colleague', colleagueName);
    // removeColleagueInput.value = '';
    document.getElementById('remove-colleague').value = '';
    const tabs = document.querySelectorAll('.tab');
    let tabIndex = -1;
    let uniqueTabId=-1
    // Find the index of the tab with the matching colleague name
    tabs.forEach((tab, index) => {
        if (tab.dataset.name === colleagueName) {
            tabIndex = index;
            uniqueTabId=tab.dataset.tabId;
        }
    });

    if (uniqueTabId !== -1) {
        
      // Gather the chat content of the colleague being removed
      const chatsToDistribute = colleaguesChats[uniqueTabId]?.innerHTML || '';
      const locationsToDistribute = locations[uniqueTabId]?.innerHTML || '';
      // Remove the tab and its content
      const tab = document.querySelector(`.tab[data-tab-id="${uniqueTabId}"]`);
      tabsContainer.removeChild(tab);
      const contents = document.querySelectorAll('.tab-content');
      const contentToRemove = document.querySelector(`.tab-content[data-tab-index="${uniqueTabId}"]`);
      tabContentsContainer.removeChild(contentToRemove);
      // Update the data structures
      delete colleaguesChats[uniqueTabId];
      delete rectangle[uniqueTabId];
      delete locations[uniqueTabId];


       // Handle tab switch if the active tab was removed
      if (tab.classList.contains('active')) {
        // If the removed tab was the active tab, select another tab
        if (tabs.length > 1) {
          const newTabIndex = (tabIndex === 0) ? 1 : tabIndex - 1; // Go to the previous tab if possible, else next
          const newActiveTab = tabs[newTabIndex];
          showTab(newActiveTab.dataset.tabId);
        } else {
          // If this was the last tab, clear the content display
          tabContentsContainer.innerHTML = '';
        }
      }    
      if (chatsToDistribute) {
        try {
          const parser = new DOMParser();
          const doc = parser.parseFromString(chatsToDistribute, 'text/html');
          
          // Select all chat containers
          const chatContainers = doc.querySelectorAll('.chat-container');
          
          chatContainers.forEach((chatContainer) => {
            const chatBoxes = chatContainer.querySelectorAll('.chat-box'); // Select all chat-boxes inside the chatContainer
        
            chatBoxes.forEach((chatBox) => {
              const messages = chatBox.querySelectorAll('.message'); // Select all messages within the chatBox
              let counter = 0
              messages.forEach((message, index) => {
                counter++;
                const messageData = {
                  timestamp: null,
                  user_id: null,
                  user_message: null,
                  admin_response: null,
                  latitude: null,
                  longitude: null,
                  location: null,
                  flag: "deleted",
                  awaiting: null,
                  name: getTabName(fromTab),
                  message_number: counter
                };
    
                // Extract data from the message element
                const userId_ = chatContainer.getAttribute('data-user-id');
                messageData.user_id = userId_;
    
                // Find the timestamp
                const timestampElement = message.querySelector('.timestamp');
                //const timestamp_ = timestampElement ? timestampElement.textContent.replace('Sent at: ', '').trim() : '';
                const timestamp_ = timestampElement ? timestampElement.textContent.replace(/^(Sent at: |A küldés ideje: )/, '').trim() : '';

                messageData.timestamp = timestamp_;
    
                // Find the user message
                const userMessageElement = message.querySelector('.user-input');
                //const userMessage_ = userMessageElement ? userMessageElement.textContent.replace('User: ', '').trim() : '';
                const userMessage_ = userMessageElement ? userMessageElement.textContent.replace(/^(User: |Ügyfél: )/, '').trim() : '';

                messageData.user_message = userMessage_;
    
                // Find the admin response
                const adminResponseElement = message.querySelector('.admin-response');
                let adminMessage_ = adminResponseElement ? adminResponseElement.textContent.trim() : '';
                // Check if the admin message starts with 'Admin:'
                // if (adminMessage_.startsWith('Admin:')) {
                //   // Remove 'Admin:' from the start of the string
                //   adminMessage_ = adminMessage_.substring(6).trim(); // Removes 'Admin:' and trims any leading space
                // }
                messageData.admin_response = adminMessage_;
                const isLastMessage = index === messages.length - 1;
                if (isLastMessage) {
                  // If it's the last message, check if chatBox has the 'awaiting-response' attribute
                  if (chatBox.classList.contains('awaiting-response')) {
                    messageData.awaiting = true;
                  } else {
                    messageData.awaiting = false;
                  }
                } else {
                  // For all other messages, set awaiting to false
                  messageData.awaiting = false;
                }



                // Process location data
                if (locationsToDistribute) {
                  const parser = new DOMParser();
                  const doc = parser.parseFromString(locationsToDistribute, 'text/html');
                  const locationBoxes = doc.querySelectorAll('.location-box');
  
                  locationBoxes.forEach((box) => {
                    const userId = box.getAttribute('data-user-id');
                    if (userId === userId_) {
                      const longitudeElement = box.querySelector('i.fa-arrows-left-right').parentElement.nextElementSibling;
                      const latitudeElement = box.querySelector('i.fa-arrows-up-down').parentElement.nextElementSibling;
                      const locationElement = box.querySelector('i.fa-location-dot').parentElement.nextElementSibling;

                      messageData.longitude = longitudeElement ? longitudeElement.textContent.trim() : 'No Data';
                      messageData.latitude = latitudeElement ? latitudeElement.textContent.trim() : 'No Data';
                      messageData.location = locationElement ? locationElement.textContent.trim() : 'No Data';
                      }
                    });
                  }
                  // Append the message data
                  //appendMessage(messageData);
                });
            });
        });
          
        } catch (error) {
            console.error("Error while processing messages:", error);
        }

        

      }
   
    }
  }
}
    

function removeColleagueforSocket_theRest(removeColleagueInput){
  const colleagueName = removeColleagueInput.colleague_name.trim();
  if (colleagueName) {
    // socket.emit('remove_colleague', colleagueName);
    // removeColleagueInput.value = '';
    document.getElementById('remove-colleague').value = '';
    const tabs = document.querySelectorAll('.tab');
    let tabIndex = -1;
    let uniqueTabId=-1
    // Find the index of the tab with the matching colleague name
    tabs.forEach((tab, index) => {
        if (tab.dataset.name === colleagueName) {
            tabIndex = index;
            uniqueTabId=tab.dataset.tabId;
        }
    });

    if (uniqueTabId !== -1) {
        
      // Gather the chat content of the colleague being removed
      const chatsToDistribute = colleaguesChats[uniqueTabId]?.innerHTML || '';
      const locationsToDistribute = locations[uniqueTabId]?.innerHTML || '';
      // Remove the tab and its content
      const tab = document.querySelector(`.tab[data-tab-id="${uniqueTabId}"]`);
      tabsContainer.removeChild(tab);
      const contents = document.querySelectorAll('.tab-content');
      const contentToRemove = document.querySelector(`.tab-content[data-tab-index="${uniqueTabId}"]`);
      tabContentsContainer.removeChild(contentToRemove);
      // Update the data structures
      delete colleaguesChats[uniqueTabId];
      delete rectangle[uniqueTabId];
      delete locations[uniqueTabId];


       // Handle tab switch if the active tab was removed
      if (tab.classList.contains('active')) {
        // If the removed tab was the active tab, select another tab
        if (tabs.length > 1) {
          const newTabIndex = (tabIndex === 0) ? 1 : tabIndex - 1; // Go to the previous tab if possible, else next
          const newActiveTab = tabs[newTabIndex];
          showTab(newActiveTab.dataset.tabId);
        } else {
          // If this was the last tab, clear the content display
          tabContentsContainer.innerHTML = '';
        }
      }    
      if (chatsToDistribute) {
        try {
          const parser = new DOMParser();
          const doc = parser.parseFromString(chatsToDistribute, 'text/html');
          
          // Select all chat containers
          const chatContainers = doc.querySelectorAll('.chat-container');
          
          chatContainers.forEach((chatContainer) => {
            const chatBoxes = chatContainer.querySelectorAll('.chat-box'); // Select all chat-boxes inside the chatContainer
        
            chatBoxes.forEach((chatBox) => {
              const messages = chatBox.querySelectorAll('.message'); // Select all messages within the chatBox
              let counter = 0
              messages.forEach((message, index) => {
                counter++;
                const messageData = {
                  timestamp: null,
                  user_id: null,
                  user_message: null,
                  admin_response: null,
                  latitude: null,
                  longitude: null,
                  location: null,
                  flag: "deleted",
                  awaiting: null,
                  name: getTabName(fromTab),
                  message_number: counter
                };
    
                // Extract data from the message element
                const userId_ = chatContainer.getAttribute('data-user-id');
                messageData.user_id = userId_;
    
                // Find the timestamp
                const timestampElement = message.querySelector('.timestamp');
                const timestamp_ = timestampElement ? timestampElement.textContent.replace(/^(Sent at: |A küldés ideje: )/, '').trim() : '';

                //const timestamp_ = timestampElement ? timestampElement.textContent.replace('Sent at: ', '').trim() : '';
                messageData.timestamp = timestamp_;
    
                // Find the user message
                const userMessageElement = message.querySelector('.user-input');
                const userMessage_ = userMessageElement ? userMessageElement.textContent.replace(/^(User: |Ügyfél: )/, '').trim() : '';

                //const userMessage_ = userMessageElement ? userMessageElement.textContent.replace('User: ', '').trim() : '';
                messageData.user_message = userMessage_;
    
                // Find the admin response
                const adminResponseElement = message.querySelector('.admin-response');
                let adminMessage_ = adminResponseElement ? adminResponseElement.textContent.trim() : '';
                // Check if the admin message starts with 'Admin:'
                // if (adminMessage_.startsWith('Admin:')) {
                //   // Remove 'Admin:' from the start of the string
                //   adminMessage_ = adminMessage_.substring(6).trim(); // Removes 'Admin:' and trims any leading space
                // }
                messageData.admin_response = adminMessage_;
                const isLastMessage = index === messages.length - 1;
                if (isLastMessage) {
                  // If it's the last message, check if chatBox has the 'awaiting-response' attribute
                  if (chatBox.classList.contains('awaiting-response')) {
                    messageData.awaiting = true;
                  } else {
                    messageData.awaiting = false;
                  }
                } else {
                  // For all other messages, set awaiting to false
                  messageData.awaiting = false;
                }



                // Process location data
                if (locationsToDistribute) {
                  const parser = new DOMParser();
                  const doc = parser.parseFromString(locationsToDistribute, 'text/html');
                  const locationBoxes = doc.querySelectorAll('.location-box');
  
                  locationBoxes.forEach((box) => {
                    const userId = box.getAttribute('data-user-id');
                    if (userId === userId_) {
                      const longitudeElement = box.querySelector('i.fa-arrows-left-right').parentElement.nextElementSibling;
                      const latitudeElement = box.querySelector('i.fa-arrows-up-down').parentElement.nextElementSibling;
                      const locationElement = box.querySelector('i.fa-location-dot').parentElement.nextElementSibling;

                      messageData.longitude = longitudeElement ? longitudeElement.textContent.trim() : 'No Data';
                      messageData.latitude = latitudeElement ? latitudeElement.textContent.trim() : 'No Data';
                      messageData.location = locationElement ? locationElement.textContent.trim() : 'No Data';
                      }
                    });
                  }
                 
                });
            });
        });
          
        } catch (error) {
            console.error("Error while processing messages:", error);
        }

        

      }
   
    }
  }
}

function updateModeUIShowTabAgain(){
    const tabsInputContainer = document.getElementById('tabs-input-container');
    const editTabsContainer = document.getElementById('edit-tabs-container');

    tabsInputContainer.style.display = 'block';
    editTabsContainer.style.display = 'none';
    
    const label = document.querySelector('label[for="colleagues"]');
    const input = document.getElementById('colleagues');
    const createTabsButton = document.getElementById('create-tabs-button');
    const tabContainer = document.querySelector('.tab-container')

    // Show the container and apply styles
    
    tabsInputContainer.classList.add('visible');
    tabsInputContainer.style.padding = '0.9%';
    tabsInputContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
    tabsInputContainer.style.borderRadius = '10px';
    tabsInputContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
    tabsInputContainer.style.marginBottom = '10px';
    tabContainer.style.height = '70vh';

    // Apply styles to the label
    label.style.color = 'white';
    label.style.fontSize = '14px';
    label.style.marginRight = '20px';

    // Apply styles to the input
    input.style.padding = '3px';
    input.style.border = 'none';
    input.style.borderRadius = '10px';
    input.style.flexGrow = '1';
    input.style.marginRight = '20px';
    input.style.fontSize = '13px';
    

    

    // Apply styles to the button
    createTabsButton.style.padding = '4px 15px'; // Button padding
    createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
    createTabsButton.style.color = 'white'; // Button text color
    createTabsButton.style.border = 'none'; // No border
    createTabsButton.style.borderRadius = '30px'; // Rounded corners
    createTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
    createTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
    
    // Add hover effect
    createTabsButton.onmouseover = function() {
        createTabsButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
    };

    createTabsButton.onmouseout = function() {
        createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
    };

    // Apply styles to the button
    editTabsButton.style.padding = '4px 15px'; // Button padding
    editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
    editTabsButton.style.color = 'white'; // Button text color
    editTabsButton.style.border = 'none'; // No border
    editTabsButton.style.borderRadius = '30px'; // Rounded corners
    editTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
    editTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
    editTabsButton.style.marginLeft = '20px';
    
    // Add hover effect
    editTabsButton.onmouseover = function() {
      editTabsButton.style.backgroundColor = '#b02a37'; // Darker green on hover
    };

    editTabsButton.onmouseout = function() {
        editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
    };
    editTabsButton.addEventListener('click', function() {
      // Toggle the visibility of the edit tabs container
          editTabsContainer.classList.add('visible');
          
          // Style the editTabsContainer similarly to tabsInputContainer
          editTabsContainer.style.padding = '0.9%';
          editTabsContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
          editTabsContainer.style.borderRadius = '10px';
          editTabsContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
          editTabsContainer.style.marginBottom = '10px';
  
      
          addColleagueButton.style.padding = '4px 15px'; // Button padding
          addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
          addColleagueButton.style.color = 'white'; // Button text color
          addColleagueButton.style.border = 'none'; // No border
          addColleagueButton.style.borderRadius = '30px'; // Rounded corners
          addColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
          addColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          addColleagueButton.style.marginRight = '20px';
          addColleagueButton.style.height='29px';
          // Add hover effect
          addColleagueButton.onmouseover = function() {
            addColleagueButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
        };
  
        addColleagueButton.onmouseout = function() {
          addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
        };
      });
  
}


function editTabCreation(){
  // Hide the initial tabs input container
  const tabsInputContainer = document.getElementById('tabs-input-container');
  const editTabsContainer = document.getElementById('edit-tabs-container');

  tabsInputContainer.style.display = 'none';
  editTabsContainer.style.display = 'block';

  editTabsContainer.style.padding = '0.9%';
  editTabsContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
  editTabsContainer.style.borderRadius = '10px';
  editTabsContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
  editTabsContainer.style.marginBottom = '10px';

  addColleagueButton.style.padding = '4px 15px'; // Button padding
  addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
  addColleagueButton.style.color = 'white'; // Button text color
  addColleagueButton.style.border = 'none'; // No border
  addColleagueButton.style.borderRadius = '30px'; // Rounded corners
  addColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
  addColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
  addColleagueButton.style.marginRight = '20px';
  addColleagueButton.style.height='29px';
    // Add hover effect
    addColleagueButton.onmouseover = function() {
    addColleagueButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
  };

  addColleagueButton.onmouseout = function() {
    addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
  };
  
  backButton.style.padding = '4px 15px'; // Button padding
  backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // 
  backButton.style.color = 'white'; // Button text color
  backButton.style.border = 'none'; // No border
  backButton.style.borderRadius = '30px'; // Rounded corners
  backButton.style.cursor = 'pointer'; // Pointer cursor on hover
  backButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
  backButton.style.height='29px';
    // Add hover effect
    backButton.onmouseover = function() {
    backButton.style.backgroundColor = '#157f4f'; // Darker green on hover
  };

  backButton.onmouseout = function() {
    backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // Reset to original color
  };

  
  removeColleagueButton.style.padding = '4px 15px'; // Button padding
  removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
  removeColleagueButton.style.color = 'white'; // Button text color
  removeColleagueButton.style.border = 'none'; // No border
  removeColleagueButton.style.borderRadius = '30px'; // Rounded corners
  removeColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
  removeColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
  removeColleagueButton.style.marginRight = '30px';
  removeColleagueButton.style.height = '29px';

    // Add hover effect
    removeColleagueButton.onmouseover = function() {
    removeColleagueButton.style.backgroundColor = '#b02a37'; // Darker green on hover
  };

  removeColleagueButton.onmouseout = function() {
    removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
  };
  const labelAddC = document.querySelector('label[for="add-colleague"]');
  const labelremoveC = document.querySelector('label[for="remove-colleague"]');
  
  const inputaddC = document.getElementById('add-colleague');
  const inputremoveC = document.getElementById('remove-colleague');
 
  // Apply styles to the label
  labelAddC.style.color = 'white';
  labelAddC.style.fontSize = '14px';
  labelAddC.style.marginRight = '2px';
  labelAddC.style.marginTop = '3px';

  // Apply styles to the label
  labelremoveC.style.color = 'white';
  labelremoveC.style.fontSize = '14px';
  labelremoveC.style.marginRight = '2px';
  labelremoveC.style.marginTop = '3px';

  inputaddC.style.padding = '3px';
  inputaddC.style.height = '27px';
  inputaddC.style.border = 'none';
  inputaddC.style.borderRadius = '10px';
  inputaddC.style.flexGrow = '1';
  inputaddC.style.marginLeft = '20px';
  inputaddC.style.marginRight = '20px';
  inputaddC.style.fontSize = '13px';
  inputaddC.style.minWidth = '40px';
  inputaddC.style.boxSizing = 'border-box';

  inputremoveC.style.padding = '3px';

  inputremoveC.style.border = 'none';
  inputremoveC.style.borderRadius = '10px';
  inputremoveC.style.flexGrow = '1';
  inputremoveC.style.marginRight = '20px';
  inputremoveC.style.marginLeft = '20px';
  inputremoveC.style.fontSize = '13px';
  inputremoveC.style.minWidth = '40px';
  inputremoveC.style.boxSizing = 'border-box';
  inputremoveC.style.height = '27px'; 

  function updateFontSize2() {
      // Ensure this targets the correct label

    // Get the viewport width
    const viewportWidth = window.innerWidth;

    // Calculate the font size (using a ratio of the viewport width)
    const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width

    // Define minimum and maximum font size
    const minFontSize = 10; // Minimum font size (in px)
    const maxFontSize = 14; // Maximum font size (in px)

    // Apply the responsive font size with the min and max limits
    const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);

    // Set the calculated font size to the label
    labelremoveC.style.fontSize = `${fontSize}px`;
    inputremoveC.style.fontSize = `${fontSize}px`;
    labelAddC.style.fontSize = `${fontSize}px`;
    inputaddC.style.fontSize = `${fontSize}px`;
    addColleagueButton.style.fontSize=`${fontSize}px`;
    backButton.style.fontSize=`${fontSize}px`;
    removeColleagueButton.style.fontSize=`${fontSize}px`;


  }


  window.addEventListener('resize', () => {
    updateFontSize2();
  });

  // Apply the function on initial load
  updateFontSize2();


      
      
}  


function updateAdminIntervention_response_ManualMode(data){
      chatBox = colleaguesChats[data.tabIndex].querySelector(`.chat-container[data-user-id="${data.user_id}"]`);
      // Update message logic remains the same


      const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
        const botResponseElement = el.querySelector('.bot-response');
        const adminResponseElement = el.querySelector('.admin-response');
      
        // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
        return (botResponseElement && 
          (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
           botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
         (adminResponseElement && 
          (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
           adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
      });

      // const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
      // const adminResponseElement = el.querySelector('.admin-response');
      // return adminResponseElement && adminResponseElement.textContent.includes('Awaiting Admin Response...');
      // });
      
      const language = localStorage.getItem('language') || 'hu';
      const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
      if (awaitingMessageElement) {
        awaitingMessageElement.querySelector('.admin-response').textContent = data.response
          ? `Admin: ${data.response}`
          : awaitingText;

        
        chatBox.classList.add('not-awaiting-response');
        chatBox.classList.remove('awaiting-response');
        const userRectangle01 = document.querySelector(`.user-rectangle[data-user-id="${ data.user_id}"]`);
        userRectangle01.classList.remove('awaiting-response'); // Remove class from user rectangle
    
        counterForManualModeAddMessage[data.tabIndex][data.user_id] += 1;   
      } else {
            chatContainer = colleaguesChats[data.tabIndex].querySelector(`.chat-container[data-user-id="${data.user_id}"]`);
            chatBox = chatContainer.querySelector('.chat-box');
            const messageElement = document.createElement('div');
            messageElement.className = 'message';
            // Create admin message content and style it to align on the left
            const adminMessageContent = `
              <div class="admin-message">
                ${data.response
                  ? data.flag === "deleted"
                    ? `<span class="admin-response">${data.response}</span>`  // If message is deleted, no Admin: prefix
                    : `<span class="admin-response">Admin: ${data.response}</span>`  // Otherwise, include Admin: prefix
                  : '<span class="admin-response"></span>'  // If no admin response, show "Awaiting Admin Response..."
                }
              </div>
            `;
            // Append user message first (right-aligned) and admin message after (left-aligned)
            messageElement.innerHTML = adminMessageContent;
            chatBox.appendChild(messageElement); 
            if (data.awaiting === false) {
              counterForManualModeAddMessage[tabIndex][message.user_id]=1;
            }  
        }
        checkAwaitingResponse(data.tabIndex)
      
    
    };





function updateModeUI(mode) {
  if (mode!=='manual'){
    cleanObjectsButKeepZero();
  }
  if (mode === 'manual') {
    const toggleButton = document.getElementById('toggle-response-mode')
    toggleButton.textContent = 'Switch to Automatic Response';
     // Update button style
    toggleButton.style.cursor = 'pointer';
    toggleButton.style.backgroundColor = '#007BFF'; // Toggle between two colors
    toggleButton.style.color = '#FFFFFF';
    toggleButton.style.width = '120px';  // Set width as defined
    toggleButton.style.height = '40px';  // Set height as defined
    toggleButton.style.border = 'none';  // Remove border
    toggleButton.style.borderRadius = '30px';  // Set rounded corners to 30px
    toggleButton.style.fontSize = '10px';  // Set font size to 10px
    toggleButton.style.marginRight = '20px';

    const tabsInputContainer = document.getElementById('tabs-input-container');
    const label = document.querySelector('label[for="colleagues"]');
    const input = document.getElementById('colleagues');
    const createTabsButton = document.getElementById('create-tabs-button');
    const tabContainer = document.querySelector('.tab-container')

    // Show the container and apply styles
    
    tabsInputContainer.classList.add('visible');
    tabsInputContainer.style.display = 'flex';
    tabsInputContainer.style.padding = '0.9%';
    tabsInputContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
    tabsInputContainer.style.borderRadius = '10px';
    tabsInputContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
    tabsInputContainer.style.marginBottom = '10px';
    tabContainer.style.height = '70vh';

    // Apply styles to the label
    label.style.color = 'white';
    //label.style.fontSize = '14px';
    label.style.marginRight = '20px';

    // Apply styles to the input
    input.style.padding = '3px';
    input.style.border = 'none';
    input.style.borderRadius = '10px';
    input.style.flexGrow = '1';
    input.style.marginRight = '20px';
    //input.style.fontSize = '13px';
    

    

    // Apply styles to the button
    createTabsButton.style.padding = '4px 15px'; // Button padding
    createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
    createTabsButton.style.color = 'white'; // Button text color
    createTabsButton.style.border = 'none'; // No border
    createTabsButton.style.borderRadius = '30px'; // Rounded corners
    createTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
    createTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
    
    // Add hover effect
    createTabsButton.onmouseover = function() {
        createTabsButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
    };

    createTabsButton.onmouseout = function() {
        createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
    };
    
    // Apply styles to the button
    editTabsButton.style.padding = '4px 15px'; // Button padding
    editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
    editTabsButton.style.color = 'white'; // Button text color
    editTabsButton.style.border = 'none'; // No border
    editTabsButton.style.borderRadius = '30px'; // Rounded corners
    editTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
    editTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
    editTabsButton.style.marginLeft = '20px';
    
    // Add hover effect
    editTabsButton.onmouseover = function() {
      editTabsButton.style.backgroundColor = '#b02a37'; // Darker green on hover
    };

    editTabsButton.onmouseout = function() {
        editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
    };

    function updateFontSize() {
        // Ensure this targets the correct label
    
      // Get the viewport width
      const viewportWidth = window.innerWidth;
    
      // Calculate the font size (using a ratio of the viewport width)
      const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width
    
      // Define minimum and maximum font size
      const minFontSize = 10; // Minimum font size (in px)
      const maxFontSize = 14; // Maximum font size (in px)
    
      // Apply the responsive font size with the min and max limits
      const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);
    
      // Set the calculated font size to the label
      label.style.fontSize = `${fontSize}px`;
      input.style.fontSize = `${fontSize}px`;
      editTabsButton.style.fontSize=`${fontSize}px`;
      createTabsButton.style.fontSize=`${fontSize}px`;
    }
    
    // Apply the function on initial load
    updateFontSize();
    
    // Update the font size whenever the window is resized
    window.addEventListener('resize', updateFontSize);
    editTabsButton.addEventListener('click', function() {
      // Toggle the visibility of the edit tabs container
          editTabsContainer.classList.add('visible');
          
          // Style the editTabsContainer similarly to tabsInputContainer
          editTabsContainer.style.padding = '0.9%';
          editTabsContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
          editTabsContainer.style.borderRadius = '10px';
          editTabsContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
          editTabsContainer.style.marginBottom = '10px';
  
      
          addColleagueButton.style.padding = '4px 15px'; // Button padding
          addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
          addColleagueButton.style.color = 'white'; // Button text color
          addColleagueButton.style.border = 'none'; // No border
          addColleagueButton.style.borderRadius = '30px'; // Rounded corners
          addColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
          addColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          addColleagueButton.style.marginRight = '20px';
          addColleagueButton.style.height='29px';
          // Add hover effect
          addColleagueButton.onmouseover = function() {
            addColleagueButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
        };
  
        addColleagueButton.onmouseout = function() {
          addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
        };
  
          
          backButton.style.padding = '4px 15px'; // Button padding
          backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // 
          backButton.style.color = 'white'; // Button text color
          backButton.style.border = 'none'; // No border
          backButton.style.borderRadius = '30px'; // Rounded corners
          backButton.style.cursor = 'pointer'; // Pointer cursor on hover
          backButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          backButton.style.height='29px';
          // Add hover effect
          backButton.onmouseover = function() {
            backButton.style.backgroundColor = '#157f4f'; // Darker green on hover
          };
  
          backButton.onmouseout = function() {
            backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // Reset to original color
          };
  
        
          removeColleagueButton.style.padding = '4px 15px'; // Button padding
          removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
          removeColleagueButton.style.color = 'white'; // Button text color
          removeColleagueButton.style.border = 'none'; // No border
          removeColleagueButton.style.borderRadius = '30px'; // Rounded corners
          removeColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
          removeColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          removeColleagueButton.style.marginRight = '30px';
          removeColleagueButton.style.height = '29px';
  
          // Add hover effect
          removeColleagueButton.onmouseover = function() {
            removeColleagueButton.style.backgroundColor = '#b02a37'; // Darker green on hover
          };
  
          removeColleagueButton.onmouseout = function() {
            removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
          };
          const labelAddC = document.querySelector('label[for="add-colleague"]');
  
          // Apply styles to the label
          labelAddC.style.color = 'white';
          //labelAddC.style.fontSize = '14px';
          labelAddC.style.marginRight = '2px';
          labelAddC.style.marginTop = '3px';
  
          const labelremoveC = document.querySelector('label[for="remove-colleague"]');
          // Apply styles to the label
          labelremoveC.style.color = 'white';
          //labelremoveC.style.fontSize = '14px';
          labelremoveC.style.marginRight = '2px';
          labelremoveC.style.marginTop = '3px';
  
          const inputaddC = document.getElementById('add-colleague');
          const inputremoveC = document.getElementById('remove-colleague');
  
          inputaddC.style.padding = '3px';
          inputaddC.style.height = '27px';
          inputaddC.style.border = 'none';
          inputaddC.style.borderRadius = '10px';
          inputaddC.style.flexGrow = '1';
          inputaddC.style.marginLeft = '20px';
          inputaddC.style.marginRight = '20px';
          //inputaddC.style.fontSize = '13px';
          inputaddC.style.minWidth = '40px';
          inputaddC.style.boxSizing = 'border-box';
  
          inputremoveC.style.padding = '3px';
        
          inputremoveC.style.border = 'none';
          inputremoveC.style.borderRadius = '10px';
          inputremoveC.style.flexGrow = '1';
          inputremoveC.style.marginRight = '20px';
          inputremoveC.style.marginLeft = '20px';
          //inputremoveC.style.fontSize = '13px';
          inputremoveC.style.minWidth = '40px';
          inputremoveC.style.boxSizing = 'border-box';
          inputremoveC.style.height = '27px'; 

          function updateFontSize2() {
            // Ensure this targets the correct label
        
          // Get the viewport width
          const viewportWidth = window.innerWidth;
        
          // Calculate the font size (using a ratio of the viewport width)
          const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width
        
          // Define minimum and maximum font size
          const minFontSize = 10; // Minimum font size (in px)
          const maxFontSize = 14; // Maximum font size (in px)
        
          // Apply the responsive font size with the min and max limits
          const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);
        
          // Set the calculated font size to the label
          labelremoveC.style.fontSize = `${fontSize}px`;
          inputremoveC.style.fontSize = `${fontSize}px`;
          labelAddC.style.fontSize = `${fontSize}px`;
          inputaddC.style.fontSize = `${fontSize}px`;
          addColleagueButton.style.fontSize=`${fontSize}px`;
          backButton.style.fontSize=`${fontSize}px`;
          removeColleagueButton.style.fontSize=`${fontSize}px`;
  
        
        }
      
        
        window.addEventListener('resize', () => {
          updateFontSize2();
        });
      
        // Apply the function on initial load
        updateFontSize2();
        
      });

     
  } else {
    const toggleButton = document.getElementById('toggle-response-mode')
    
    // Hide the container when in automatic mode
    tabsInputContainer.classList.remove('visible');
    tabsInputContainer.style.display = 'none';
    editTabsContainer.style.display = 'none';
    // tabContainer.style.height = '80vh';
    const language = localStorage.getItem('language') || 'hu';
    toggleButton.textContent = manualMode ? translations[language]['toggleModeManual'] : translations[language]['toggleMode'];
    
    // toggleButton.textContent = manualMode ? 'Switch to Automatic Response' : 'Switch to Manual Response';
     // Update button style
    toggleButton.style.cursor = 'pointer';
    toggleButton.style.backgroundColor = manualMode ? '#007BFF' : '#28A745'; // Toggle between two colors
    toggleButton.style.color = '#FFFFFF';
    toggleButton.style.width = '120px';  // Set width as defined
    toggleButton.style.height = '40px';  // Set height as defined
    toggleButton.style.border = 'none';  // Remove border
    toggleButton.style.borderRadius = '30px';  // Set rounded corners to 30px
    toggleButton.style.fontSize = '10px';  // Set font size to 10px
    toggleButton.style.marginRight = '20px';
    createDefaultTabForAutomaticMode()
  }
  toggleButton.addEventListener('click', handleModeSwitch);
}


async function updateModeUI_promise(mode) {
  return new Promise((resolve) => {
    if (mode !== 'manual') {
      cleanObjectsButKeepZero();
    }

    if (mode === 'manual') {
      const toggleButton = document.getElementById('toggle-response-mode');
      toggleButton.textContent = 'Switch to Automatic Response';
      // Update button style
      toggleButton.style.cursor = 'pointer';
      toggleButton.style.backgroundColor = '#007BFF';
      toggleButton.style.color = '#FFFFFF';
      toggleButton.style.width = '120px';
      toggleButton.style.height = '40px';
      toggleButton.style.border = 'none';
      toggleButton.style.borderRadius = '30px';
      toggleButton.style.fontSize = '10px';
      toggleButton.style.marginRight = '20px';

      const tabsInputContainer = document.getElementById('tabs-input-container');
      const label = document.querySelector('label[for="colleagues"]');
      const input = document.getElementById('colleagues');
      const createTabsButton = document.getElementById('create-tabs-button');
      const tabContainer = document.querySelector('.tab-container');

      // Show the container and apply styles
      tabsInputContainer.classList.add('visible');
      tabsInputContainer.style.display = 'flex';
      tabsInputContainer.style.padding = '0.9%';
      tabsInputContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
      tabsInputContainer.style.borderRadius = '10px';
      tabsInputContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
      tabsInputContainer.style.marginBottom = '10px';
      tabContainer.style.height = '70vh';

      // Apply styles to the label
      label.style.color = 'white';
      label.style.fontSize = '14px';
      label.style.marginRight = '20px';

      // Apply styles to the input
      input.style.padding = '3px';
      input.style.border = 'none';
      input.style.borderRadius = '10px';
      input.style.flexGrow = '1';
      input.style.marginRight = '20px';
      input.style.fontSize = '13px';

      // Apply styles to the button
      createTabsButton.style.padding = '4px 15px';
      createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))';
      createTabsButton.style.color = 'white';
      createTabsButton.style.border = 'none';
      createTabsButton.style.borderRadius = '30px';
      createTabsButton.style.cursor = 'pointer';
      createTabsButton.style.transition = 'background-color 0.3s ease';

      createTabsButton.onmouseover = function () {
        createTabsButton.style.backgroundColor = '#0d6efd';
      };

      createTabsButton.onmouseout = function () {
        createTabsButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))';
      };

      // More style and event handling code here (for other buttons)
      // Apply styles to the button
    editTabsButton.style.padding = '4px 15px'; // Button padding
    editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
    editTabsButton.style.color = 'white'; // Button text color
    editTabsButton.style.border = 'none'; // No border
    editTabsButton.style.borderRadius = '30px'; // Rounded corners
    editTabsButton.style.cursor = 'pointer'; // Pointer cursor on hover
    editTabsButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
    editTabsButton.style.marginLeft = '20px';
    
    // Add hover effect
    editTabsButton.onmouseover = function() {
      editTabsButton.style.backgroundColor = '#b02a37'; // Darker green on hover
    };

    editTabsButton.onmouseout = function() {
        editTabsButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
    };

    function updateFontSize() {
        // Ensure this targets the correct label
    
      // Get the viewport width
      const viewportWidth = window.innerWidth;
    
      // Calculate the font size (using a ratio of the viewport width)
      const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width
    
      // Define minimum and maximum font size
      const minFontSize = 10; // Minimum font size (in px)
      const maxFontSize = 14; // Maximum font size (in px)
    
      // Apply the responsive font size with the min and max limits
      const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);
    
      // Set the calculated font size to the label
      label.style.fontSize = `${fontSize}px`;
      input.style.fontSize = `${fontSize}px`;
      editTabsButton.style.fontSize=`${fontSize}px`;
      createTabsButton.style.fontSize=`${fontSize}px`;
    }
    
    // Apply the function on initial load
    updateFontSize();
    
    // Update the font size whenever the window is resized
    window.addEventListener('resize', updateFontSize);
    editTabsButton.addEventListener('click', function() {
      // Toggle the visibility of the edit tabs container
          editTabsContainer.classList.add('visible');
          
          // Style the editTabsContainer similarly to tabsInputContainer
          editTabsContainer.style.padding = '0.9%';
          editTabsContainer.style.backgroundColor = 'rgba(30, 30, 30, 0.7)';
          editTabsContainer.style.borderRadius = '10px';
          editTabsContainer.style.boxShadow = '0 4px 15px rgba(0, 0, 0, 0.3)';
          editTabsContainer.style.marginBottom = '10px';
  
      
          addColleagueButton.style.padding = '4px 15px'; // Button padding
          addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Button background color
          addColleagueButton.style.color = 'white'; // Button text color
          addColleagueButton.style.border = 'none'; // No border
          addColleagueButton.style.borderRadius = '30px'; // Rounded corners
          addColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
          addColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          addColleagueButton.style.marginRight = '20px';
          addColleagueButton.style.height='29px';
          // Add hover effect
          addColleagueButton.onmouseover = function() {
            addColleagueButton.style.backgroundColor = '#0d6efd'; // Darker green on hover
        };
  
        addColleagueButton.onmouseout = function() {
          addColleagueButton.style.background = 'linear-gradient(to right, rgba(151, 188, 252, 0.7), rgba(216, 228, 250, 0.7))'; // Reset to original color
        };
  
          
          backButton.style.padding = '4px 15px'; // Button padding
          backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // 
          backButton.style.color = 'white'; // Button text color
          backButton.style.border = 'none'; // No border
          backButton.style.borderRadius = '30px'; // Rounded corners
          backButton.style.cursor = 'pointer'; // Pointer cursor on hover
          backButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          backButton.style.height='29px';
          // Add hover effect
          backButton.onmouseover = function() {
            backButton.style.backgroundColor = '#157f4f'; // Darker green on hover
          };
  
          backButton.onmouseout = function() {
            backButton.style.background = 'linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7))'; // Reset to original color
          };
  
        
          removeColleagueButton.style.padding = '4px 15px'; // Button padding
          removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Button background color
          removeColleagueButton.style.color = 'white'; // Button text color
          removeColleagueButton.style.border = 'none'; // No border
          removeColleagueButton.style.borderRadius = '30px'; // Rounded corners
          removeColleagueButton.style.cursor = 'pointer'; // Pointer cursor on hover
          removeColleagueButton.style.transition = 'background-color 0.3s ease'; // Smooth transition for hover effect
          removeColleagueButton.style.marginRight = '30px';
          removeColleagueButton.style.height = '29px';
  
          // Add hover effect
          removeColleagueButton.onmouseover = function() {
            removeColleagueButton.style.backgroundColor = '#b02a37'; // Darker green on hover
          };
  
          removeColleagueButton.onmouseout = function() {
            removeColleagueButton.style.background = 'linear-gradient(to right, rgba(252, 151, 151, 0.7), rgba(250, 216, 216, 0.7))'; // Reset to original color
          };
          const labelAddC = document.querySelector('label[for="add-colleague"]');
  
          // Apply styles to the label
          labelAddC.style.color = 'white';
          //labelAddC.style.fontSize = '14px';
          labelAddC.style.marginRight = '2px';
          labelAddC.style.marginTop = '3px';
  
          const labelremoveC = document.querySelector('label[for="remove-colleague"]');
          // Apply styles to the label
          labelremoveC.style.color = 'white';
          //labelremoveC.style.fontSize = '14px';
          labelremoveC.style.marginRight = '2px';
          labelremoveC.style.marginTop = '3px';
  
          const inputaddC = document.getElementById('add-colleague');
          const inputremoveC = document.getElementById('remove-colleague');
  
          inputaddC.style.padding = '3px';
          inputaddC.style.height = '27px';
          inputaddC.style.border = 'none';
          inputaddC.style.borderRadius = '10px';
          inputaddC.style.flexGrow = '1';
          inputaddC.style.marginLeft = '20px';
          inputaddC.style.marginRight = '20px';
          //inputaddC.style.fontSize = '13px';
          inputaddC.style.minWidth = '40px';
          inputaddC.style.boxSizing = 'border-box';
  
          inputremoveC.style.padding = '3px';
        
          inputremoveC.style.border = 'none';
          inputremoveC.style.borderRadius = '10px';
          inputremoveC.style.flexGrow = '1';
          inputremoveC.style.marginRight = '20px';
          inputremoveC.style.marginLeft = '20px';
          //inputremoveC.style.fontSize = '13px';
          inputremoveC.style.minWidth = '40px';
          inputremoveC.style.boxSizing = 'border-box';
          inputremoveC.style.height = '27px'; 

          function updateFontSize2() {
            // Ensure this targets the correct label
        
          // Get the viewport width
          const viewportWidth = window.innerWidth;
        
          // Calculate the font size (using a ratio of the viewport width)
          const calculatedFontSize = viewportWidth * 0.02; // Example ratio: 4% of viewport width
        
          // Define minimum and maximum font size
          const minFontSize = 10; // Minimum font size (in px)
          const maxFontSize = 14; // Maximum font size (in px)
        
          // Apply the responsive font size with the min and max limits
          const fontSize = Math.min(Math.max(calculatedFontSize, minFontSize), maxFontSize);
        
          // Set the calculated font size to the label
          labelremoveC.style.fontSize = `${fontSize}px`;
          inputremoveC.style.fontSize = `${fontSize}px`;
          labelAddC.style.fontSize = `${fontSize}px`;
          inputaddC.style.fontSize = `${fontSize}px`;
          addColleagueButton.style.fontSize=`${fontSize}px`;
          backButton.style.fontSize=`${fontSize}px`;
          removeColleagueButton.style.fontSize=`${fontSize}px`;
  
        
        }
      
        
        window.addEventListener('resize', () => {
          updateFontSize2();
        });
      
        // Apply the function on initial load
        updateFontSize2();
        
      });


      resolve(); // Resolve the promise when the UI update is complete
    } else {
      const toggleButton = document.getElementById('toggle-response-mode');

      // Hide the container when in automatic mode
      tabsInputContainer.classList.remove('visible');
      editTabsContainer.style.display = 'none';
      tabsInputContainer.style.display = 'none';

      const language = localStorage.getItem('language') || 'hu';
      toggleButton.textContent = manualMode ? translations[language]['toggleModeManual'] : translations[language]['toggleMode'];
    
      // toggleButton.textContent = mode === 'manual' ? 'Switch to Automatic Response' : 'Switch to Manual Response';
      toggleButton.style.cursor = 'pointer';
      toggleButton.style.backgroundColor = mode === 'manual' ? '#007BFF' : '#28A745';
      toggleButton.style.color = '#FFFFFF';
      toggleButton.style.width = '120px';
      toggleButton.style.height = '40px';
      toggleButton.style.border = 'none';
      toggleButton.style.borderRadius = '30px';
      toggleButton.style.fontSize = '10px';
      toggleButton.style.marginRight = '20px';
      createDefaultTabForAutomaticMode();
      
      resolve(); // Resolve the promise here as well
    }
  });
}





function updateAdminIntervention(data){    // update the rectangles' state getting from the socket
  console.log("szerintem ide")
  const language = localStorage.getItem('language') || 'hu';
  //Old before language option: userButtons[data.user_id].textContent = automaticResponseStates[data.user_id] ? 'Automatic Response' : 'Admin Intervention';
  userButtons[data.user_id].textContent = automaticResponseStates[data.user_id] ? translations[language]['manualIntervention'] : translations[language]['automaticResponse'];
  function updateButtonStyles(adminResponseButton, isAutomaticResponseNeeded) {
    if (isAutomaticResponseNeeded) {
        adminResponseButton.style.backgroundColor = '#007bff'; // Blue for Automatic Response
        adminResponseButton.onmouseover = () => adminResponseButton.style.backgroundColor = '#0056b3'; // Darker blue on hover
        adminResponseButton.onmouseout = () => adminResponseButton.style.backgroundColor = '#007bff'; // Original blue color
    } else {
        adminResponseButton.style.backgroundColor = '#28a745'; // Green for Admin Intervention
        adminResponseButton.onmouseover = () => adminResponseButton.style.backgroundColor = '#218838'; // Darker green on hover
        adminResponseButton.onmouseout = () => adminResponseButton.style.backgroundColor = '#28a745'; // Original green color
    }
}
  updateButtonStyles(userButtons[data.user_id], automaticResponseStates[data.user_id]); // Update styles based on new state

    // Immediate hover effect if hovering during click
    userButtons[data.user_id].style.backgroundColor = automaticResponseStates[data.user_id] ? '#0056b3' : '#218838';

  // Togle the admin icon on the rectangle
  const userRectangle = rectangle_automatic[data.user_id];
  const avatar = userRectangle.querySelector('.human-avatar');

  if (automaticResponseStates[data.user_id]) {
    
  } else {
   
  }



  userButtonStates[data.user_id] = automaticResponseStates[data.user_id] ? 'automaticResponse' : 'adminIntervention';
  chatBox = Chats_automatic[data.user_id];


  if(automaticResponseStates[data.user_id]){

    // Manual mode → show avatar
    avatar.style.display = "flex";
    
    const chatContainer = document.querySelector(`.chat-container[data-user-id="${data.user_id}"]`);
    // Get the chatBox within the chatContainer
    const chatBox = chatContainer ? chatContainer.querySelector('.chat-box_automatic') : null;
    if (chatBox) {
      // Adjust the height to account for the admin response controls
      chatBox.style.height = 'calc(100% - 60px)';
    }
    // Create the admin response controls to be fixed at the bottom
    const adminResponseControls = document.createElement('div');
    adminResponseControls.className = 'admin-response-controls';
    adminResponseControls.innerHTML = `
      <textarea class="manual-response" placeholder="${language === 'hu' ? 'Írd ide az üzneted...' : 'Type your response...'}"></textarea>
      <button class="send-response">${language === 'hu' ? 'Küldés' : 'Send Response'}</button>
    `;
    adminResponseControls.style.display = 'flex';
    adminResponseControls.style.alignItems = 'flex-start';
    chatContainer.appendChild(adminResponseControls);

    // Add click event listener to send the response
    const responseInput = adminResponseControls.querySelector('.manual-response');
    const sendButton = adminResponseControls.querySelector('.send-response');

    // Set the initial height and style of the textarea
    responseInput.style.overflowY = 'hidden'; // Hide vertical scrollbar
    responseInput.style.resize = 'none'; // Prevent manual resizing
    responseInput.style.height = '30px'; // Set an initial height (min-height)

    // Auto-expand the textarea only when it exceeds the initial height
    responseInput.addEventListener('input', function () {
      this.style.height = '30px'; // Reset the height to the initial value
      if (this.scrollHeight > this.clientHeight) {
        this.style.height = (this.scrollHeight) + 'px'; // Expand if the content overflows
      }
    });

    async function sendResponse() {
      const response = responseInput.value;
      if (response) {
        const timestamp = new Date().toISOString();
        try {
          console.log("RESP -*-*-*-*-*-*-*-*-*-*-*-: ", response)
          console.log("DATA: ", data)
            socket.emit('admin_response', { user_id: data.user_id, response: response, timestamp: timestamp });
            await handleAdminResponse(chatBox, response, counterForAddAdminMessage[data.user_id]);
        } catch (error) {
            console.error("Error in handleAdminResponse:", error);
        }
        responseInput.value = '';
        responseInput.style.height = '30px';
        
      }
  }

    sendButton.addEventListener('click', sendResponse);

    // Add 'Enter' key event listener for input box
    responseInput.addEventListener('keypress', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        sendResponse();
      }
    });




    
    const messageElement = document.createElement('div');
    messageElement.className = 'message';

    // Create admin message content and style it to align on the left
   
    const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';

    
    const adminMessageContent = `
      <div class="admin-message">
        <span class="admin-response">${awaitingText}</span>
      </div>
    `;

    // Append user message first (right-aligned) and admin message after (left-aligned)
    messageElement.innerHTML = adminMessageContent;

    // Get all messages in chatBox
    const messages = chatBox.querySelectorAll('.message');

    // Find the last .message element
    const lastMessage = messages.length > 0 ? messages[messages.length - 1] : null;

    let shouldAppend = true; // Default to appending

    if (lastMessage) {
      // Find .admin-message inside the last message
      const lastAdminMessage = lastMessage.querySelector('.admin-message');
    
      if (lastAdminMessage) {
          // Find .admin-response inside .admin-message
          const lastAdminResponse = lastAdminMessage.querySelector('.bot-response');
          
          if (lastAdminResponse) {
              const lastAdminText = lastAdminResponse.textContent.trim();
              
              // If the last message is the awaiting response, don't append
              if (lastAdminText === 'Adminisztrátori válaszra várakozás...' || lastAdminText === 'Awaiting Admin Response...') {
                  shouldAppend = false;
              }
          }
      }
    }
    
    if (shouldAppend) {
      chatBox.appendChild(messageElement);
    }
    chatBox.classList.add('awaiting-response');
    rectangle_automatic[data.user_id].classList.add('awaiting-response'); // Add class to user rectangle
    counterForAddAdminMessage[data.user_id] = 0
  }else{
       // Automatic mode → hide avatar
    avatar.style.display = "none";
    removeAdminResponseControls(data.user_id)


 

    const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
    const botResponseElement = el.querySelector('.bot-response');
    const adminResponseElement = el.querySelector('.admin-response');
  
    // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
    return (botResponseElement && 
      (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
        botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
      (adminResponseElement && 
      (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
        adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
  });
  // We have awaiting message
  if (awaitingMessageElement) {
    awaitingMessageElement.remove();
  }
  chatBox.classList.remove('awaiting-response');
  userRectangle.classList.remove('awaiting-response');







  }
}




function updateAdminIntervention_fornewlyjoined(data, stateparameter){
  const language = localStorage.getItem('language') || 'hu';
  userButtons[data].textContent = stateparameter ? translations[language]['manualIntervention'] : translations[language]['automaticResponse'];
  function updateButtonStyles(adminResponseButton, isAutomaticResponseNeeded) {
    if (isAutomaticResponseNeeded) {
        adminResponseButton.style.backgroundColor = '#007bff'; // Blue for Automatic Response
        adminResponseButton.onmouseover = () => adminResponseButton.style.backgroundColor = '#0056b3'; // Darker blue on hover
        adminResponseButton.onmouseout = () => adminResponseButton.style.backgroundColor = '#007bff'; // Original blue color
    } else {
        adminResponseButton.style.backgroundColor = '#28a745'; // Green for Admin Intervention
        adminResponseButton.onmouseover = () => adminResponseButton.style.backgroundColor = '#218838'; // Darker green on hover
        adminResponseButton.onmouseout = () => adminResponseButton.style.backgroundColor = '#28a745'; // Original green color
    }
}
  updateButtonStyles(userButtons[data], stateparameter); // Update styles based on new state

  
  // Togle the admin icon on the rectangle
  const userRectangle = rectangle_automatic[data];
  const avatar = userRectangle.querySelector('.human-avatar');

  // if (automaticResponseStates[data]) {
  //     // Manual mode → show avatar
  //     avatar.style.display = "flex";
  // } else {
  //     // Automatic mode → hide avatar
  //     avatar.style.display = "none";
  // }

    // Immediate hover effect if hovering during click
    userButtons[data].style.backgroundColor = stateparameter ? '#0056b3' : '#218838';

  userButtonStates[data] = stateparameter ? 'automaticResponse' : 'adminIntervention';
  chatBox = Chats_automatic[data];


  if(stateparameter){
    avatar.style.display = "flex";
    const chatContainer = document.querySelector(`.chat-container[data-user-id="${data}"]`);

   
    // Get the chatBox within the chatContainer
    const chatBox = chatContainer ? chatContainer.querySelector('.chat-box_automatic') : null;
    if (chatBox) {
      // Adjust the height to account for the admin response controls
      chatBox.style.height = 'calc(100% - 60px)';
    }
    // Create the admin response controls to be fixed at the bottom
    const adminResponseControls = document.createElement('div');
    adminResponseControls.className = 'admin-response-controls';
    adminResponseControls.innerHTML = `
      <textarea class="manual-response" placeholder="${language === 'hu' ? 'Írd ide az üzneted...' : 'Type your response...'}"></textarea>
      <button class="send-response">${language === 'hu' ? 'Küldés' : 'Send Response'}</button>
    `;


    adminResponseControls.style.display = 'flex';
    adminResponseControls.style.alignItems = 'flex-start';
    chatContainer.appendChild(adminResponseControls);

    // Add click event listener to send the response
    const responseInput = adminResponseControls.querySelector('.manual-response');
    const sendButton = adminResponseControls.querySelector('.send-response');

    // Set the initial height and style of the textarea
    responseInput.style.overflowY = 'hidden'; // Hide vertical scrollbar
    responseInput.style.resize = 'none'; // Prevent manual resizing
    responseInput.style.height = '30px'; // Set an initial height (min-height)

    // Auto-expand the textarea only when it exceeds the initial height
    responseInput.addEventListener('input', function () {
      this.style.height = '30px'; // Reset the height to the initial value
      if (this.scrollHeight > this.clientHeight) {
        this.style.height = (this.scrollHeight) + 'px'; // Expand if the content overflows
      }
    });

    async function sendResponse() {
      const response = responseInput.value;
      if (response) {
        const timestamp = new Date().toISOString();
        try {
            console.log("NEM HINNÉM %%%%%%%%%%%%%%")
            socket.emit('admin_response', { user_id: data, response: response, timestamp: timestamp });
            await handleAdminResponse(chatBox, response, counterForAddAdminMessage[data]);
        } catch (error) {
            console.error("Error in handleAdminResponse:", error);
        }
        responseInput.value = '';
        responseInput.style.height = '30px';
        
      }
  }

    sendButton.addEventListener('click', sendResponse);

    // Add 'Enter' key event listener for input box
    responseInput.addEventListener('keypress', function (event) {
      if (event.key === 'Enter') {
        event.preventDefault();
        sendResponse();
      }
    });




    
    const messageElement = document.createElement('div');
    messageElement.className = 'message';

    // Create admin message content and style it to align on the left
  

    const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';

    
    const adminMessageContent = `
      <div class="admin-message">
        <span class="admin-response">${awaitingText}</span>
      </div>
    `;

    // Append user message first (right-aligned) and admin message after (left-aligned)
    messageElement.innerHTML = adminMessageContent;
    chatBox.appendChild(messageElement);
    chatBox.classList.remove('not-awaiting-response');
    chatBox.classList.add('awaiting-response');
    rectangle_automatic[data].classList.add('awaiting-response'); // Add class to user rectangle
  }else{
    avatar.style.display = "none";
    removeAdminResponseControls(data)


    

 

    const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
    const botResponseElement = el.querySelector('.bot-response');
    const adminResponseElement = el.querySelector('.admin-response');
  
    // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
    return (botResponseElement && 
      (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
        botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
      (adminResponseElement && 
      (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
        adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
      });
      // We have awaiting message
      if (awaitingMessageElement) {
        awaitingMessageElement.remove();
      }
      chatBox.classList.remove('awaiting-response');
      userRectangle.classList.remove('awaiting-response');


  }
}

function updateAdminIntervention_response(data){
  console.log(data)
  console.log("UPDATEADMININTERV????")
  chatBox = Chats_automatic[data.user_id];
  console.log(chatBox)
  // Update message logic remains the same


  // const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
  // const adminResponseElement = el.querySelector('.admin-response');
  // return adminResponseElement && adminResponseElement.textContent.includes('Awaiting Admin Response...');
  // });

  const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
    const botResponseElement = el.querySelector('.bot-response');
    const adminResponseElement = el.querySelector('.admin-response');
  
    // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
    return (botResponseElement && 
      (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
       botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
     (adminResponseElement && 
      (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
       adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
  });


  const language = localStorage.getItem('language') || 'hu';
  const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
  if (awaitingMessageElement) {
    console.log("Van awaiting element ----- !!!")
    const responseElement = awaitingMessageElement.querySelector('.admin-response, .bot-response');
    if (responseElement) {
      responseElement.textContent = data.response ? `Admin: ${data.response}` : awaitingText;
    }


    
    chatBox.classList.add('not-awaiting-response');
    chatBox.classList.remove('awaiting-response');
    rectangle_automatic[data.user_id].classList.remove('awaiting-response'); // Remove class from user rectangle
    counterForAddAdminMessage[data.user_id] += 1;
    
    
  } else {

        console.log("Nincs awaiting element ----- !!!")

        const messageElement = document.createElement('div');
        messageElement.className = 'message';
        // Create admin message content and style it to align on the left
        const adminMessageContent = `
        <div class="admin-message">
          ${data.response ? `<span class="admin-response">Admin: ${data.response}</span>` : '<span class="admin-response"> </span>'}
        </div>
        `;
  
        // Append user message first (right-aligned) and admin message after (left-aligned)
        messageElement.innerHTML = adminMessageContent;
  
        chatBox.appendChild(messageElement);
        chatBox.scrollTo({
          top: chatBox.scrollHeight,
          behavior: 'smooth'
        });

      
      
      
    }
  

};

async function createTabsForClient(tabs) {
  return new Promise(resolve => {
    isTabCreated = true;
    const tabsData = [];
    
    // Clear previous tabs and contents
    tabsContainer.innerHTML = '';
    tabContentsContainer.innerHTML = '';
    

    // Clear the chats map
    Object.keys(colleaguesChats).forEach(key => delete colleaguesChats[key]);

    tabs.forEach((tabData, index) => {
      const { name, uniqueId } = tabData; // Extract name and uniqueId from the tabData
      tabsData.push({ name, uniqueId });
      const tab = document.createElement('div');
      tab.classList.add('tab', 'clickable');
      tab.dataset.tabId = uniqueId;
      tab.dataset.name = name;

      // Set display style to flex to align items in a row
      tab.style.display = 'flex';
      tab.style.alignItems = 'center'; // Vertically center the items

      const tabName = document.createElement('span');
      tabName.textContent = name;
      tabName.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
      tab.appendChild(tabName);

      const exclamationMark = document.createElement('span');
      exclamationMark.classList.add('exclamation-mark');
      exclamationMark.textContent = '!';
      exclamationMark.style.color = 'red';
      exclamationMark.style.display = 'none'; // Initially hidden
      exclamationMark.style.marginLeft = '8px'; // Adjust spacing
      exclamationMark.style.fontWeight = 'bold';
      exclamationMark.style.fontSize = 'clamp(7px, 0.8vw, 12px)';

      tab.appendChild(exclamationMark);

      tab.onclick = () => showTab(uniqueId);

      tab.ondblclick = () => {
        const currentName = tabName.textContent;

        // Create an input field to replace the tab name
        const input = document.createElement('input');
        input.type = 'text';
        input.value = String(currentName);
        input.style.fontSize = 'clamp(7px, 0.8vw, 12px)';
        input.style.width = '100%'; // Adjust to fit tab width
        input.style.border = 'none';
        input.style.outline = 'none';
        input.style.background = 'transparent';
        input.style.color = 'inherit';
        input.style.textAlign = 'center';
       

        // Replace the tab name with the input field
        tab.replaceChild(input, tabName);

        // Focus and select the input field
        input.focus();
        input.select();

        // Save the new name when the input loses focus or Enter is pressed
        const saveName = () => {
          const newName = String(input.value.trim());

          // If the name is not empty, update it
          if (newName) {
            tabName.textContent = newName;

            // Update the tabsData array for this tab
            const tabData = tabsData.find(t => t.uniqueId === uniqueId);
            if (tabData) tabData.name = newName;

            socket.emit('update_tab_name', { uniqueId, newName });
          }

          // Replace the input field with the updated name
          tab.replaceChild(tabName, input);
          input.value = ''; // Clear the value after saving
        };

        // Handle saving when Enter is pressed
        input.onkeypress = (e) => {
          if (e.key === 'Enter') saveName();
        };

        // Handle saving when focus is lost
        input.onblur = saveName;
      };

      if (index === 0) tab.classList.add('active'); // Set the first tab as active
      tabsContainer.appendChild(tab);

      // Create tab content
      const content = document.createElement('div');
      content.classList.add('tab-content');
      content.dataset.tabIndex = uniqueId; // Associate content with tab index

      // Initially, hide all tab contents except the first
      if (index !== 0) content.style.display = 'none';

      // Create Grid Layout for Tab Content with 2 rows and 3 columns
      const topRow = document.createElement('div');
      topRow.classList.add('top-row');

      const language = localStorage.getItem('language') || 'hu';
    

      const topLeftSection = document.createElement('div');
      topLeftSection.classList.add('top-left-section');
      topLeftSection.textContent = translations[language]['customers'] || 'Ügyfelek';
      // topLeftSection.textContent = 'Customers';
      topLeftSection.style.fontWeight = 'bold';

      const topMiddleSection = document.createElement('div');
      topMiddleSection.classList.add('top-middle-section');
      //topMiddleSection.textContent = 'Chats';
      topMiddleSection.textContent = language === 'hu' ? 'Üzenetek' : 'Chats';
      topMiddleSection.style.fontWeight = 'bold';

      const topRightSection = document.createElement('div');
      topRightSection.classList.add('top-right-section');
      topRightSection.textContent = translations[language]['customerDetails'] || 'Customer details'; // Fallback to English if translation is not found
    
      // topRightSection.textContent = 'Customer details';
      topRightSection.style.fontWeight = 'bold';

      window.addEventListener('resize', () => {
        // Calculate new font size based on window width
        let newFontSize = window.innerWidth / 50; // Adjust as needed
        // Ensure it doesn't exceed the maximum size
        newFontSize = Math.min(14, Math.max(newFontSize, 10));
        topLeftSection.style.fontSize = `${newFontSize}px`;
        topMiddleSection.style.fontSize = `${newFontSize}px`;
        topRightSection.style.fontSize = `${newFontSize}px`;
      });

      topRow.appendChild(topLeftSection);
      topRow.appendChild(topMiddleSection);
      topRow.appendChild(topRightSection);

      const bottomRow = document.createElement('div');
      bottomRow.classList.add('bottom-row');

      const bottomLeftSection = document.createElement('div');
      bottomLeftSection.classList.add('bottom-left-section');

      const bottomMiddleSection = document.createElement('div');
      bottomMiddleSection.classList.add('bottom-middle-section');

      const bottomRightSection = document.createElement('div');
      bottomRightSection.classList.add('bottom-right-section');

      bottomRow.appendChild(bottomLeftSection);
      bottomRow.appendChild(bottomMiddleSection);
      bottomRow.appendChild(bottomRightSection);

      content.appendChild(topRow);
      content.appendChild(bottomRow);
      tabContentsContainer.appendChild(content);

      colleaguesChats[uniqueId] = bottomMiddleSection; // Map index to the middle bottom content element where chatboxes go
      rectangle[uniqueId] = bottomLeftSection;
      locations[uniqueId] = bottomRightSection;
      counterForManualModeAddMessage[uniqueId] = {};

      const inputField = document.getElementById('colleagues');
      if (inputField) {
        inputField.value = '';
      } else {
        console.warn('Input field #colleagues not found when trying to clear.');
      }
    });

    
      resolve(); // Signal that tab creation is complete
 
  });
}





















// EVERYBODY WHO THE MESSAGE WAS EMMITTED BUT NOT DRAGED AN DROP
async function appendMessageSavedStates(message, tabUniqueID, specialArg = null){
  console.log("KEZDŐDIK --------------")
  if (specialArg) {
      socket.emit('log_message_distribution', {
          message: message,
          tab_uniqueId: tabUniqueID,
          specialArg: specialArg,
          timestamp : new Date().toISOString()
      });
  }
//Convert timestamp to UTC Date
if (message.timestamp) {
    let parsedDate;

    if (typeof message.timestamp === "number") {
        // assume UNIX seconds → convert to ms
        parsedDate = new Date(message.timestamp * 1000);
    } else if (!isNaN(Date.parse(message.timestamp))) {
        // already a valid date string
        parsedDate = new Date(message.timestamp);
    }

    if (parsedDate && !isNaN(parsedDate)) {
        message.timestamp = parsedDate.toISOString();
    } else {
        console.warn("Invalid timestamp, leaving as is:", message.timestamp);
    }
} else {
    console.warn("No timestamp found in message");
}

// Option 1: Save as ISO UTC string

  const formattedTime = formatTimestampForClient(message.timestamp);

  const language = localStorage.getItem('language') || 'hu';
  const translation_Sent_User = {
    sentAT: language === 'hu' ? 'A Küldés ideje' : 'Sent at',
    User: language === 'hu' ? 'Ügyfél' : 'User',
    };
  
  let chatBox = null;
  let tabContentForChatBox = null;
  let tabIndex=null;
  tabIndex=tabUniqueID
  // Get the bottom-left section for the current tab index
  if (!rectangle[tabIndex]) {
    console.error(`rectangle[tabIndex] is undefined for tabIndex: ${tabIndex}`);

  }
  const bottomLeftSection = rectangle[tabIndex];

  // Check if a rectangle for the user already exists
  let userRectangle = document.querySelector(`.user-rectangle[data-user-id="${message.user_id}"]`);
  if (!userRectangle) {
    // Create a new rectangle for the user in the bottom-left section
    userRectangle = document.createElement('div');
    userRectangle.className = 'user-rectangle';
    userRectangle.dataset.userId = message.user_id;
    userRectangle.dataset.tabIndex = tabIndex;
    userRectangle.style.height = '50px';
    userRectangle.style.width = '100%'; // Assume it fills the width of the first column
    if (message.flag) {
      userRectangle.dataset.flag = 'true';
      userRectangle.classList.add('default-green');
    } else {
      userRectangle.dataset.flag = 'false';
      userRectangle.classList.add('default-blue');
    }
   
    // const computedStyle = window.getComputedStyle(userRectangle);
    // userRectangle.dataset.originalBackground = computedStyle.background;
    userRectangle.style.borderBottom = '1px solid rgb(65, 75, 95)';
    userRectangle.style.cursor = 'pointer';
    userRectangle.style.display = 'flex';
    userRectangle.style.alignItems = 'center';
    userRectangle.style.padding = '0 10px';
    userRectangle.style.boxSizing = 'border-box';
    userRectangle.style.flexShrink = '0';
    
    const truncatedUserId = message.user_id.substring(0, 8);
    userRectangle.textContent = ` ${truncatedUserId}`;
    const colorUser = getPastelColor();
    //userRectangle.innerHTML = `<i class="fa-solid fa-user" style="margin-right: 10px; color: ${colorUser}"></i> ${truncatedUserId}`;
    userRectangle.innerHTML = `
        <div style="display: flex; align-items: center;">
          <i class="fa-solid fa-user"
            style="margin-right: 10px;
                    font-size: clamp(8px, 1.2vw, 15px);
                    color: ${colorUser};"></i>
          <span style="
            font-size: clamp(10px, 1.2vw, 15px);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          ">
            ${truncatedUserId}
          </span>
        </div>`;
    
      // Add drag-and-drop events
      userRectangle.draggable = true; // Enable native dragging
      userRectangle.addEventListener('dragstart', (e) => {
        const selectedIds = Array.from(document.querySelectorAll('.ctrl-click-selected'))
          .map(rect => rect.dataset.userId);

        const userIdsToDrag = selectedIds.length > 0 ? selectedIds : [message.user_id]; // Handle multiple or single
        e.dataTransfer.setData('text/plain', JSON.stringify({ userIds: userIdsToDrag, fromTab: tabIndex }));
        userRectangle.style.opacity = '1';
      });



      document.querySelectorAll('.tab').forEach((tab) => {
        tab.addEventListener('dragenter', (e) => {
          e.preventDefault();
          tab.classList.add('highlight');
        });

        tab.addEventListener('dragover', (e) => {
          e.preventDefault();
        });

        tab.addEventListener('dragleave', () => {
          tab.classList.remove('highlight');
        });

        tab.addEventListener('drop', (e) => {
          e.preventDefault();
          tab.classList.remove('highlight');
    
          const data = JSON.parse(e.dataTransfer.getData('text/plain'));
          const fromTab = data.fromTab;
          const userIds = data.userIds;
          const toTab = tab.dataset.tabId; // Assuming tabs have a `data-uniqueId`
          
          

          if (fromTab !== toTab) {
            // Sort userIds based on their arrival time (earliest first)
            userIds.sort((a, b) => {
                const rectA = document.querySelector(`.user-rectangle[data-user-id="${a}"]`);
                const rectB = document.querySelector(`.user-rectangle[data-user-id="${b}"]`);
                
                // Get arrival times, falling back to current time if not found
                const timeA = rectA ? new Date(rectA.dataset.arrivalTime).getTime() : Date.now();
                const timeB = rectB ? new Date(rectB.dataset.arrivalTime).getTime() : Date.now();

                return timeA - timeB; // Sort in ascending order (earliest first)
            });

            // Now handle the rectangles in order of arrival time
            userIds.forEach(userId => {
                const rectangleToMove = document.querySelector(`.user-rectangle[data-user-id="${userId}"]`);
                if (rectangleToMove) {
                    // Use existing function for individual rectangles
                    manageRectangleDragAndDrop(userId, fromTab, toTab);
                    rectangleToMove.classList.remove('ctrl-click-selected');
                    rectangleToMove.style.background = rectangleToMove.dataset.originalBackground;
                      }
                  });
                  checkAwaitingResponse(fromTab);
                  checkAwaitingResponse(toTab);

                  
              }
          });
      });


            // Add click event to display the user's chatbox in the middle section
        userRectangle.addEventListener('click', (event) => {

        const userId = userRectangle.dataset.userId;
        if (event.ctrlKey) {
            // Ctrl + Click detected
         
            

            // Toggle red background on Ctrl + Click
            if (userRectangle.classList.contains('ctrl-click-selected')) {
                userRectangle.classList.remove('ctrl-click-selected');
                if (userRectangle.getAttribute('data-flag')=== 'true'){
                  userRectangle.classList.remove('user-rectangle-hover-lightgreen');
                  userRectangle.classList.add('default-green');
                }else{
                  userRectangle.classList.remove('user-rectangle-hover-lightblue');
                  userRectangle.classList.add('default-blue');
                }
                
            } else {
                userRectangle.classList.add('ctrl-click-selected');
               
                if (userRectangle.getAttribute('data-flag')=== 'true'){
                  userRectangle.classList.remove('default-green');
                  userRectangle.classList.add('user-rectangle-hover-lightgreen');
                }else{
                  userRectangle.classList.remove('default-blue');
                  userRectangle.classList.add('user-rectangle-hover-lightblue');
                }
               
                showUserChatBox(message.user_id, tabIndex);
                showLocationBox(message.user_id, tabIndex);
            
            }

            event.preventDefault();
            event.stopPropagation();
            return; // Prevent further logic
            }

  
        // Regular click logic
        const currentActiveRectangle = activeRectangles[tabIndex];
        if (currentActiveRectangle) {
          if (currentActiveRectangle !== userRectangle){
            if (currentActiveRectangle.getAttribute('data-flag')=== 'true'){   
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightgreen');
              currentActiveRectangle.classList.add('default-green');
            }else{
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightblue');
              currentActiveRectangle.classList.add('default-blue');
            }
            if (userRectangle.getAttribute('data-flag')=== 'true'){
              userRectangle.classList.remove('default-green');
              userRectangle.classList.add('user-rectangle-hover-lightgreen');
            }else{
              userRectangle.classList.remove('default-blue');
              userRectangle.classList.add('user-rectangle-hover-lightblue');
            }
            showUserChatBox(message.user_id, tabIndex);
            showLocationBox(message.user_id, tabIndex);
            activeRectangles[tabIndex] = userRectangle;
            isUserRectangleClickedPerTab[tabIndex] = true;
            
          }
          if(currentActiveRectangle === userRectangle){
            if (currentActiveRectangle.getAttribute('data-flag')=== 'true'){
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightgreen');
              userRectangle.classList.add('default-green');
            }else{
              currentActiveRectangle.classList.remove('user-rectangle-hover-lightblue');
              userRectangle.classList.add('default-blue');
            }
            activeRectangles[tabIndex] = null;
            isUserRectangleClickedPerTab[tabIndex] = false;
            
          }   
        }else{
          activeRectangles[tabIndex] = userRectangle;
          if (userRectangle.getAttribute('data-flag')=== 'true'){
            userRectangle.classList.remove('default-green');
            userRectangle.classList.add('user-rectangle-hover-lightgreen');
          }else{
            userRectangle.classList.remove('default-blue');
            userRectangle.classList.add('user-rectangle-hover-lightblue');
          }

          showUserChatBox(message.user_id, tabIndex);
          showLocationBox(message.user_id, tabIndex);
          isUserRectangleClickedPerTab[tabIndex] = true;
          
        }
      });
      
            userRectangle.addEventListener('mouseover', () => {
              // Only apply hover if this rectangle is not the active one
              if (!activeRectangles || activeRectangles[tabIndex] == null) {
                if (message.flag) {
                  userRectangle.classList.remove('default-green');
                  userRectangle.classList.add('user-rectangle-hover-lightgreen');
              } else {
                userRectangle.classList.remove('default-blue');
                userRectangle.classList.add('user-rectangle-hover-lightblue');
                }
            }
        
            // Only apply hover if this rectangle is not the active one
            if (userRectangle !== activeRectangles?.[tabIndex]) {
                if (message.flag) {
                    userRectangle.classList.remove('default-green');
                    userRectangle.classList.add('user-rectangle-hover-lightgreen');
                } else {
                    userRectangle.classList.remove('default-blue');
                    userRectangle.classList.add('user-rectangle-hover-lightblue');
                }
            }
        });
              userRectangle.addEventListener('mouseout', () => {
                // Only remove hover if this rectangle is not the active one
                if (userRectangle !== activeRectangles[tabIndex]) {
                  if (!userRectangle.classList.contains('ctrl-click-selected')){
                    if (message.flag){
                      userRectangle.classList.remove('user-rectangle-hover-lightgreen');
                      userRectangle.classList.add('default-green');
                    }else{
                      userRectangle.classList.remove('user-rectangle-hover-lightblue');
                      userRectangle.classList.add('default-blue');
                    }
                  
                  }
                   
                    
                }
            });
 
        // Append the rectangle to the top of the bottom-left section
        bottomLeftSection.prepend(userRectangle); // Use prepend to add to the top
        // if(isUserRectangleClickedPerTab[tabIndex]){
        //   bottomLeftSection.scrollTop = bottomLeftSection.scrollHeight;
        // } 
        }

      // Logic to create or find the user's chat container in the bottom-middle section
      let existingChatContainer = null;

      for (const index in colleaguesChats) {
        const tabContent = colleaguesChats[index];
        const chatContainerInTab = tabContent.querySelector(`.chat-container[data-user-id="${message.user_id}"]`);
        if (chatContainerInTab) {
          existingChatContainer = chatContainerInTab;
          tabContentForChatBox = tabContent;
          
          break;
        }
      }
 
  if (!existingChatContainer) {
    tabContentForChatBox = colleaguesChats[tabIndex];
    existingChatContainer = createChatBox(message.user_id, tabContentForChatBox, tabIndex);
    existingChatContainer.querySelector('.chat-box').classList.remove('not-awaiting-response');
    existingChatContainer.querySelector('.chat-box').classList.add('awaiting-response');

    const chatContainers = tabContentForChatBox.querySelectorAll('.chat-container');
    if (chatContainers.length > 1) {
      existingChatContainer.style.display = 'none'; // Hide the new chat container
    } else {
      existingChatContainer.style.display = 'block'; // Show the first chat container
    }
    if (message.awaiting === false) {
      userRectangle.classList.remove('awaiting-response');
    }


  } 
  chatBox = existingChatContainer.querySelector('.chat-box');

    console.log("---   NÉZZÜK  -------- @@@")
    console.log(message)


    console.log("---   NÉZZÜK1  -------- @@@")

      const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
        const botResponseElement = el.querySelector('.bot-response');
        const adminResponseElement = el.querySelector('.admin-response');
      
        // Return the message element that contains 'Awaiting Admin Response...' in either bot-response or admin-response
        return (botResponseElement && 
          (botResponseElement.textContent.includes('Awaiting Admin Response...') || 
           botResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'))) ||
         (adminResponseElement && 
          (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
           adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...')));
      });

      console.log("---   NÉZZÜK2  -------- @@@")
      
    
 
      // const language = localStorage.getItem('language') || 'hu';
      const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
      console.log("---   NÉZZÜK22  -------- @@@")
      if (awaitingMessageElement) {
        console.log("---   NÉZZÜK3  -------- @@@")
        const responseElement = awaitingMessageElement.querySelector('.admin-response, .bot-response');
        if (responseElement) {
          const responseText = message.response || message.admin_response;
          responseElement.textContent = responseText ? `Admin: ${responseText}` : awaitingText;
        }


      
    
    console.log("---   NÉZZÜK4  -------- @@@")
    chatBox.classList.add('not-awaiting-response');
    chatBox.classList.remove('awaiting-response');
    userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
    counterForManualModeAddMessage[tabIndex][message.user_id]+=1;
    
    console.log("---   BENT AWAING  -------- @@@")

    if (!message.admin_response) {
      console.log("---   Nincs admin  -------- @@@")
      const adminResponseElement = awaitingMessageElement.querySelector('.admin-response, .bot-response');
      if (adminResponseElement) {
            const parentAdminMessage = adminResponseElement.closest('.admin-message');
            if (parentAdminMessage) {
                parentAdminMessage.remove();
            }
        }

      const messageElement = document.createElement('div');
      messageElement.className = 'message';

      const timestamp = message.timestamp || '';

      const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
      let headlineContent = "";
      let headlineText = language === 'hu' 
            ? "Átvett üzenet a következő kollégától:" 
            : "Messages from ";
      if (message.flag === "deleted" && message.message_number === 1) {
        headlineContent = `
            <div class="headline-message" style="background: linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7));">
                ${headlineText} ${message.name}
            </div>
        `;

    }

        console.log("---   Itt 1  -------- @@@")
    
        // <span class="user-id">User ID: ${message.user_id}</span>  taken out after timestamp
      // Create user message content and style it to align on the right
      const userMessageContent = `
        <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
          <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
          
          <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
        </div>
      `;
      // Create admin message content and style it to align on the left
      const adminMessageContent = `
        <div class="admin-message">
          ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : `<span class="admin-response">${awaitingText}</span>`}
        </div>
      `;

      // Append user message first (right-aligned) and admin message after (left-aligned)
      // messageElement.innerHTML = userMessageContent + adminMessageContent;
      
      messageElement.innerHTML =
      (headlineContent ? headlineContent : '') +
      userMessageContent +
      adminMessageContent;

      console.log("---   Itt 2  -------- @@@")
      /////     ------    INSERTING THE MESSAGE     -----------
       
      
      const hiddenTimestampSpan = document.createElement('span');
      hiddenTimestampSpan.className = 'hidden-timestamp';
      hiddenTimestampSpan.style.display = 'none';
      hiddenTimestampSpan.textContent = timestamp;
      messageElement.appendChild(hiddenTimestampSpan);

        // Chronologically insert message based on normalized timestamps
      const newMessageTimestamp = new Date(timestamp);
      const existingMessages = Array.from(chatBox.getElementsByClassName('message'));

      let inserted = false;
      for (const existingMessage of existingMessages) {
        const hiddenTs = existingMessage.querySelector('.hidden-timestamp');
        if (hiddenTs) {
          const existingTs = new Date(hiddenTs.textContent.trim());
          if (newMessageTimestamp < existingTs) {
            chatBox.insertBefore(messageElement, existingMessage);
            inserted = true;
            break;
          }
        }
      }

      if (!inserted) {
        chatBox.appendChild(messageElement);
      }

      chatBox.classList.add('awaiting-response');
      chatBox.classList.remove('not-awaiting-response');
      userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
      counterForManualModeAddMessage[tabIndex][message.user_id]=0;
    } 
    
    // THE FIRST MESSAGE or SECONDLINE MESSAGE from THE USER HAS ARRIVED
    console.log("---   Itt 3  -------- @@@")
  } else {
      console.log("TAlán itt????@@@@@")
      if (message.user_message){   // brand new user message
        console.log("bejön a messagebe?")
      
        const messageElement = document.createElement('div');
        messageElement.className = 'message';

        const timestamp = message.timestamp || '';

        let headlineContent = "";
        let headlineText = language === 'hu' 
        ? "Átvett üzenet a következő kollégától:" 
        : "Messages from ";
          if (message.flag === "deleted" && message.message_number === 1) {
            headlineContent = `
                <div class="headline-message" style="background: linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7));">
                    ${headlineText} ${message.name}
                </div>
            `;

        }

      
        // <span class="user-id">User ID: ${message.user_id}</span> taken out after timestamp
        // Create user message content and style it to align on the right
        const userMessageContent = `
          <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
            <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
      
            <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
          </div>
        `;
        // Create admin message content and style it to align on the left
        // const adminMessageContent = `
        //   <div class="admin-message">
        //     ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : '<span class="admin-response">Awaiting Admin Response...</span>'}
        //   </div>
        // `;

        const adminMessageContent = `
          <div class="admin-message">
            ${
              message.bot_message
                ? message.bot_message === "Awaiting Admin Response..."
                  ? `<span class="admin-response">${message.bot_message}</span>` // If bot_message is "Awaiting Admin Response...", no Bot: prefix
                  : `<span class="admin-response">Bot: ${message.bot_message}</span>` // Otherwise, include Bot: prefix
                : message.admin_response
                  ? message.flag === "deleted"
                    ? `<span class="admin-response">${message.admin_response}</span>` // If message is deleted, no Admin: prefix
                    : `<span class="admin-response">Admin: ${message.admin_response}</span>` // Otherwise, include Admin: prefix
                  : '<span class="admin-response">Awaiting Admin Response...</span>' // If neither bot_message nor admin_response, show "Awaiting Admin Response..."
            }
          </div>
        `;

      

        // Append user message first (right-aligned) and admin message after (left-aligned)
        // messageElement.innerHTML = userMessageContent + adminMessageContent;
        messageElement.innerHTML =
        (headlineContent ? headlineContent : '') +
        userMessageContent +
        adminMessageContent;

        // Append hidden timestamp AFTER .innerHTML is set
        const hiddenTimestampSpan = document.createElement('span');
        hiddenTimestampSpan.className = 'hidden-timestamp';
        hiddenTimestampSpan.style.display = 'none';
        hiddenTimestampSpan.textContent = timestamp;
        messageElement.appendChild(hiddenTimestampSpan);


          
        // Insert based on timestamp ordering
        const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
        const insertBeforeIndex = chatMessages.findIndex(el => {
          const ts = el.querySelector('.hidden-timestamp');
          return ts && new Date(ts.textContent) > new Date(timestamp);
        });

        if (insertBeforeIndex === -1) {
          chatBox.appendChild(messageElement);
        } else {
          chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
        }



        chatBox.classList.add('awaiting-response');
        chatBox.classList.remove('not-awaiting-response');
        userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
        counterForManualModeAddMessage[tabIndex][message.user_id]=0;
        // Check if bot_message exists and is not "Awaiting Admin Response..."
        if (message.bot_message && message.bot_message !== "Awaiting Admin Response...") {
          // Valid bot response: Remove awaiting-response classes
          chatBox.classList.remove('awaiting-response');
          userRectangle.classList.remove('awaiting-response');
          counterForManualModeAddMessage[tabIndex][message.user_id]=1;
        }
      
        if (message.awaiting === false) {
          chatBox.classList.remove('awaiting-response');
          userRectangle.classList.remove('awaiting-response');
          counterForManualModeAddMessage[tabIndex][message.user_id]=1;
        }
        
      }else { //ITT A MÁSODIK ADMIN MESSAGET ADJUK HOZZÁ AMIKOR MÁR NINCS KIÍRVA, HOGY ADMIN VÁLSZRA VÁRVA...
        console.log("vagy itt landol?")
        const messageElement = document.createElement('div');
        messageElement.className = 'message';

        const timestamp = message.timestamp || '';

      // chatBox.classList.add('not-awaiting-response');
      // chatBox.classList.remove('awaiting-response');
      // userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
      // if (counterForManualModeAddMessage[tabIndex][message.user_id]>0){
        // Create admin message content and style it to align on the left
        const adminMessageContent = `
          <div class="admin-message">
            ${message.admin_response
              ? message.flag === "deleted"
                ? `<span class="admin-response">${message.admin_response}</span>`  // If message is deleted, no Admin: prefix
                : `<span class="admin-response">Admin: ${message.admin_response}</span>`  // Otherwise, include Admin: prefix
              : '<span class="admin-response"></span>'  // If no admin response, show "Awaiting Admin Response..."
            }
          </div>
        `;
        // Append user message first (right-aligned) and admin message after (left-aligned)
        messageElement.innerHTML = adminMessageContent;

        console.log("vagy itt landol?2")
        // Append real hidden timestamp element
        const hiddenTimestampSpan = document.createElement('span');
        hiddenTimestampSpan.className = 'hidden-timestamp';
        hiddenTimestampSpan.style.display = 'none';
        hiddenTimestampSpan.textContent = timestamp;
        messageElement.appendChild(hiddenTimestampSpan);
        console.log("vagy itt landol?3")
        // Insert into chatBox sorted by timestamp
        const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
        console.log("vagy itt landol?4")
        const insertBeforeIndex = chatMessages.findIndex(el => {
          const tsEl = el.querySelector('.hidden-timestamp');
          return tsEl && new Date(tsEl.textContent) > new Date(timestamp);
        });
        console.log("vagy itt landol?5")
        if (insertBeforeIndex === -1) {
          console.log("vagy itt landol?55")
          chatBox.appendChild(messageElement);
        } else {
          console.log("vagy itt landol?555")
          chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
        }
        console.log("vagy itt landol?6")
        if (message.awaiting === false) {
          counterForManualModeAddMessage[tabIndex][message.user_id]=1;
        }
        
    }
  }
  console.log("vagy itt landol?7")
  checkAwaitingResponse(tabIndex)

  // Get the bottom-right section for the current tab index
  const bottomRightSection = locations[tabIndex];

  // Check if a location box for the user already exists
  let userLocationBox = bottomRightSection.querySelector(`.location-box[data-user-id="${message.user_id}"]`);

  if (!userLocationBox) {
    // Create a new location box for the user if it does not exist
    userLocationBox = document.createElement('div');
    userLocationBox.className = 'location-box';
    userLocationBox.dataset.userId = message.user_id;
    userLocationBox.dataset.tabIndex = tabIndex;
    userLocationBox.style.overflowY = 'auto';
    userLocationBox.style.padding = '10px';
    userLocationBox.style.marginBottom = '5px';
    bottomRightSection.appendChild(userLocationBox);

    // Define translations
    const translation_location = {
      userID: language === 'hu' ? 'Ügyfélazonosító' : 'User-ID',
      location: language === 'hu' ? 'Hely' : 'Location',
      longitude: language === 'hu' ? 'Hosszúság' : 'Longitude',
      latitude: language === 'hu' ? 'Szélesség' : 'Latitude'
      };
   


    // Update the content of the location box with user data, including null values
    userLocationBox.innerHTML =  `
    <div style="margin-bottom: 10px;">
      <div class="user-id-header">
        <i class="fa-solid fa-user" style="margin-right: 5px;"></i>
        <span class="userID_text">${translation_location.userID}</span>
      </div>

      <div class="paddingleft" style="margin-top: 5px; font-size: clamp(7px, 0.8vw, 12px);">
        <span class="full-user-id-locbox">${message.user_id !== null ? message.user_id : 'No Data'}</span>
        <span class="truncated-user-id-locbox">${message.user_id !== null ? message.user_id.substring(0, 8) : 'No Data'}</span>
      </div>
    </div>
    <div style="margin-bottom: 10px;">
      <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(8px, 1.2vw, 15px); border-radius: 4px;">
        <i class="fa-solid fa-location-dot" style="margin-right: 5px;"></i>
        <span class="location_userlocationbox">${translation_location.location}</span>
      </div>
      <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(7px, 0.8vw, 12px);">
        
        ${message.location ?? 'No Data'}
      </div>
    </div>
     
    <div id="location-map" style="width: 100%; height: 200px; margin-top: 10px; border-radius: 4px;"></div>

    <div style="margin-bottom: 10px;">
      <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(5px, 1.2vw, 15px); border-radius: 4px;">
        <i class="fa-solid fa-arrows-left-right" style="margin-right: 5px;"></i>
        <span class="longitude_userlocationbox">${translation_location.longitude}</span>
      </div>
      <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
        
        ${message.longitude ?? 'No Data'}
      </div>
    </div>
    <div style="margin-bottom: 10px;">
      <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(5px, 1.2vw, 15px); bold; border-radius: 4px;">
        <i class="fa-solid fa-arrows-up-down" style="margin-right: 5px;"></i>  
        <span class="latitude_userlocationbox">${translation_location.latitude}</span>
      </div>
      <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
        
        ${message.latitude ?? 'No Data'}
      </div>
    </div>
   
  `;

  bottomRightSection.prepend(userLocationBox);
  // Initialize the map immediately after appending the content
  const mapContainer = userLocationBox.querySelector("#location-map");

  if (message.latitude !== null && message.longitude !== null) {
    const map = L.map(mapContainer).setView([message.latitude, message.longitude], 13);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);

    L.marker([message.latitude, message.longitude]).addTo(map)
      .bindPopup("Location: " + message.location)
      .openPopup();

    // Ensure Leaflet resizes properly
    setTimeout(() => {
      map.invalidateSize();
      }, 300);

      requestAnimationFrame(() => {
          map.invalidateSize();
      });

      // Observe element visibility and refresh map when shown
      const visibilityObserver = new IntersectionObserver(entries => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                map.invalidateSize();
            }
        });
    }, { threshold: 0.1 });

    visibilityObserver.observe(userLocationBox);

    // Handle window resize events
    L.DomEvent.on(window, 'resize', () => map.invalidateSize());

  } else {
      // Hide the map container if no valid coordinates exist
      userLocationBox.querySelector("#location-map").style.display = "none";
  }

  if (bottomRightSection.querySelectorAll('.location-box').length > 1) {
    userLocationBox.style.display = 'none';
  }
    
  }


  userRectangle.addEventListener('click', () => {
    showLocationBox(message.user_id, tabIndex);
  });

  // // Show the latest chat container in the middle section
  // showUserChatBox(message.user_id, tabIndex);
  console.log("vagy itt landol?8")
  await new Promise(requestAnimationFrame);
}

// EVERYBODY NOT INITIATED THE MESSAGE EMISSION AND MESSAGES ARE DRAGGED AND DROPPED
async function appendMessageSavedStatesForDragAndDropSocket(message, tabUniqueID, specialArg = null){
  console.log("drag&drop for not initiated")
  const formattedTime = formatTimestampForClient(message.timestamp);

  if (message.timestamp) {
    const messageDateUTC = new Date(message.timestamp * 1000); // convert seconds to ms
    if (!isNaN(messageDateUTC)) {
        message.timestamp = messageDateUTC.toISOString();
    } else {
        console.warn("Invalid timestamp, leaving as is");
    }
} else {
    console.warn("No timestamp found in message");
}
  

  const rectangleToMove = rectangle[specialArg]?.querySelector(`[data-user-id="${message.user_id}"]`);
  const chatToMove = colleaguesChats[specialArg]?.querySelector(`[data-user-id="${message.user_id}"]`);
  const locationToMove = locations[specialArg]?.querySelector(`[data-user-id="${message.user_id}"]`);
 
  if (rectangleToMove){   
      rectangle[specialArg]?.removeChild(rectangleToMove);
    }
  
  if (chatToMove){  
    // Remove the chat container from the source tab
    colleaguesChats[specialArg]?.removeChild(chatToMove);
    }
  if (locationToMove){
    // Remove the location box from the source tab
    locations[specialArg]?.removeChild(locationToMove);
  }


  const language = localStorage.getItem('language') || 'hu';
  const translation_Sent_User = {
    sentAT: language === 'hu' ? 'A Küldés ideje' : 'Sent at',
    User: language === 'hu' ? 'Ügyfél' : 'User',
    };
    
  
  let chatBox = null;
  let tabContentForChatBox = null;
  let tabIndex=null;

  tabIndex=tabUniqueID
  // Get the bottom-left section for the current tab index
  if (!rectangle[tabIndex]) {
    console.error(`rectangle[tabIndex] is undefined for tabIndex: ${tabIndex}`);
  }
  const bottomLeftSection = rectangle[tabIndex];

  // Check if a rectangle for the user already exists
  let userRectangle = document.querySelector(`.user-rectangle[data-user-id="${message.user_id}"]`);
  if (!userRectangle) {
    // Create a new rectangle for the user in the bottom-left section
    userRectangle = document.createElement('div');
    userRectangle.className = 'user-rectangle';
    userRectangle.dataset.userId = message.user_id;
    userRectangle.dataset.tabIndex = tabIndex;
    userRectangle.style.height = '50px';
    userRectangle.style.width = '100%'; // Assume it fills the width of the first column
    if (message.flag) {
      userRectangle.dataset.flag = 'true';
      userRectangle.classList.add('default-green');
    } else {
      userRectangle.dataset.flag = 'false';
      userRectangle.classList.add('default-blue');
    }
   
    userRectangle.style.borderBottom = '1px solid rgb(65, 75, 95)';
    userRectangle.style.cursor = 'pointer';
    userRectangle.style.display = 'flex';
    userRectangle.style.alignItems = 'center';
    userRectangle.style.padding = '0 10px';
    userRectangle.style.boxSizing = 'border-box';
    userRectangle.style.flexShrink = '0';
    
    const truncatedUserId = message.user_id.substring(0, 8);
    userRectangle.textContent = ` ${truncatedUserId}`;
    const colorUser = getPastelColor();
    //userRectangle.innerHTML = `<i class="fa-solid fa-user" style="margin-right: 10px; color: ${colorUser}"></i> ${truncatedUserId}`;
    userRectangle.innerHTML = `
        <div style="display: flex; align-items: center;">
          <i class="fa-solid fa-user"
            style="margin-right: 10px;
                    font-size: clamp(8px, 1.2vw, 15px);
                    color: ${colorUser};"></i>
          <span style="
            font-size: clamp(10px, 1.2vw, 15px);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
          ">
            ${truncatedUserId}
          </span>
        </div>`;
    


     // Add drag-and-drop events
     userRectangle.draggable = true; // Enable native dragging
     userRectangle.addEventListener('dragstart', (e) => {
       const selectedIds = Array.from(document.querySelectorAll('.ctrl-click-selected'))
         .map(rect => rect.dataset.userId);

       const userIdsToDrag = selectedIds.length > 0 ? selectedIds : [message.user_id]; // Handle multiple or single
       e.dataTransfer.setData('text/plain', JSON.stringify({ userIds: userIdsToDrag, fromTab: tabIndex }));
       userRectangle.style.opacity = '1';
     });



     document.querySelectorAll('.tab').forEach((tab) => {
       tab.addEventListener('dragenter', (e) => {
         e.preventDefault();
         tab.classList.add('highlight');
       });

       tab.addEventListener('dragover', (e) => {
         e.preventDefault();
       });

       tab.addEventListener('dragleave', () => {
         tab.classList.remove('highlight');
       });

       tab.addEventListener('drop', (e) => {
         e.preventDefault();
         tab.classList.remove('highlight');
   
         const data = JSON.parse(e.dataTransfer.getData('text/plain'));
         const fromTab = data.fromTab;
         const userIds = data.userIds;
         const toTab = tab.dataset.tabId; // Assuming tabs have a `data-uniqueId`
         
         

         if (fromTab !== toTab) {
           // Sort userIds based on their arrival time (earliest first)
           userIds.sort((a, b) => {
               const rectA = document.querySelector(`.user-rectangle[data-user-id="${a}"]`);
               const rectB = document.querySelector(`.user-rectangle[data-user-id="${b}"]`);
               
               // Get arrival times, falling back to current time if not found
               const timeA = rectA ? new Date(rectA.dataset.arrivalTime).getTime() : Date.now();
               const timeB = rectB ? new Date(rectB.dataset.arrivalTime).getTime() : Date.now();

               return timeA - timeB; // Sort in ascending order (earliest first)
           });

           // Now handle the rectangles in order of arrival time
           userIds.forEach(userId => {
               const rectangleToMove = document.querySelector(`.user-rectangle[data-user-id="${userId}"]`);
               if (rectangleToMove) {
                   // Use existing function for individual rectangles
                   manageRectangleDragAndDrop(userId, fromTab, toTab);
                   rectangleToMove.classList.remove('ctrl-click-selected');
                   rectangleToMove.style.background = rectangleToMove.dataset.originalBackground;
               }
            });
            checkAwaitingResponse(fromTab);
            checkAwaitingResponse(toTab);

      
       }
   });
});



     // Add click event to display the user's chatbox in the middle section
     userRectangle.addEventListener('click', (event) => {
      const userId = userRectangle.dataset.userId;
      if (event.ctrlKey) {
          // Ctrl + Click detected
       
          

          // Toggle red background on Ctrl + Click
          if (userRectangle.classList.contains('ctrl-click-selected')) {
              userRectangle.classList.remove('ctrl-click-selected');
              if (userRectangle.getAttribute('data-flag')=== 'true'){
                userRectangle.classList.remove('user-rectangle-hover-lightgreen');
                userRectangle.classList.add('default-green');
              }else{
                userRectangle.classList.remove('user-rectangle-hover-lightblue');
                userRectangle.classList.add('default-blue');
              }
              
          } else {
              userRectangle.classList.add('ctrl-click-selected');
             
              if (userRectangle.getAttribute('data-flag')=== 'true'){
                userRectangle.classList.remove('default-green');
                userRectangle.classList.add('user-rectangle-hover-lightgreen');
              }else{
                userRectangle.classList.remove('default-blue');
                userRectangle.classList.add('user-rectangle-hover-lightblue');
              }
             
              showUserChatBox(message.user_id, tabIndex);
              showLocationBox(message.user_id, tabIndex);
          
          }

          event.preventDefault();
          event.stopPropagation();
          return; // Prevent further logic
          }


      // Regular click logic
      const currentActiveRectangle = activeRectangles[tabIndex];
      if (currentActiveRectangle) {
        if (currentActiveRectangle !== userRectangle){
          if (currentActiveRectangle.getAttribute('data-flag')=== 'true'){   
            currentActiveRectangle.classList.remove('user-rectangle-hover-lightgreen');
            currentActiveRectangle.classList.add('default-green');
          }else{
            currentActiveRectangle.classList.remove('user-rectangle-hover-lightblue');
            currentActiveRectangle.classList.add('default-blue');
          }
          if (userRectangle.getAttribute('data-flag')=== 'true'){
            userRectangle.classList.remove('default-green');
            userRectangle.classList.add('user-rectangle-hover-lightgreen');
          }else{
            userRectangle.classList.remove('default-blue');
            userRectangle.classList.add('user-rectangle-hover-lightblue');
          }
          showUserChatBox(message.user_id, tabIndex);
          showLocationBox(message.user_id, tabIndex);
          activeRectangles[tabIndex] = userRectangle;
          isUserRectangleClickedPerTab[tabIndex] = true;
          
        }
        if(currentActiveRectangle === userRectangle){
          if (currentActiveRectangle.getAttribute('data-flag')=== 'true'){
            currentActiveRectangle.classList.remove('user-rectangle-hover-lightgreen');
            userRectangle.classList.add('default-green');
          }else{
            currentActiveRectangle.classList.remove('user-rectangle-hover-lightblue');
            userRectangle.classList.add('default-blue');
          }
          activeRectangles[tabIndex] = null;
          isUserRectangleClickedPerTab[tabIndex] = false;
          
        }   
      }else{
        activeRectangles[tabIndex] = userRectangle;
        if (userRectangle.getAttribute('data-flag')=== 'true'){
          userRectangle.classList.remove('default-green');
          userRectangle.classList.add('user-rectangle-hover-lightgreen');
        }else{
          userRectangle.classList.remove('default-blue');
          userRectangle.classList.add('user-rectangle-hover-lightblue');
        }

        showUserChatBox(message.user_id, tabIndex);
        showLocationBox(message.user_id, tabIndex);
        isUserRectangleClickedPerTab[tabIndex] = true;
        
      }
    });


    userRectangle.addEventListener('mouseover', () => {
      // Only apply hover if this rectangle is not the active one
      if (!activeRectangles || activeRectangles[tabIndex] == null) {
        if (message.flag) {
          userRectangle.classList.remove('default-green');
          userRectangle.classList.add('user-rectangle-hover-lightgreen');
      } else {
        userRectangle.classList.remove('default-blue');
        userRectangle.classList.add('user-rectangle-hover-lightblue');
        }
    }

    // Only apply hover if this rectangle is not the active one
    if (userRectangle !== activeRectangles?.[tabIndex]) {
        if (message.flag) {
            userRectangle.classList.remove('default-green');
            userRectangle.classList.add('user-rectangle-hover-lightgreen');
        } else {
            userRectangle.classList.remove('default-blue');
            userRectangle.classList.add('user-rectangle-hover-lightblue');
        }
    }
});
      userRectangle.addEventListener('mouseout', () => {
        // Only remove hover if this rectangle is not the active one
        if (userRectangle !== activeRectangles[tabIndex]) {
          if (!userRectangle.classList.contains('ctrl-click-selected')){
            if (message.flag){
              userRectangle.classList.remove('user-rectangle-hover-lightgreen');
              userRectangle.classList.add('default-green');
            }else{
              userRectangle.classList.remove('user-rectangle-hover-lightblue');
              userRectangle.classList.add('default-blue');
            }
          
          }
           
            
        }
    });

        // Append the rectangle to the top of the bottom-left section
        bottomLeftSection.prepend(userRectangle); // Use prepend to add to the top
        // if(isUserRectangleClickedPerTab[tabIndex]){
        //   bottomLeftSection.scrollTop = bottomLeftSection.scrollHeight;
        // } 
    
  }

  // Logic to create or find the user's chat container in the bottom-middle section
  let existingChatContainer = null;

  for (const index in colleaguesChats) {
    const tabContent = colleaguesChats[index];
    const chatContainerInTab = tabContent.querySelector(`.chat-container[data-user-id="${message.user_id}"]`);
    if (chatContainerInTab) {
      existingChatContainer = chatContainerInTab;
      tabContentForChatBox = tabContent;
      
      break;
    }
  }

  if (!existingChatContainer) {
    tabContentForChatBox = colleaguesChats[tabIndex];
    existingChatContainer = createChatBox(message.user_id, tabContentForChatBox, tabIndex);
    existingChatContainer.querySelector('.chat-box').classList.remove('not-awaiting-response');
    existingChatContainer.querySelector('.chat-box').classList.add('awaiting-response');

    const chatContainers = tabContentForChatBox.querySelectorAll('.chat-container');
    if (chatContainers.length > 1) {
      existingChatContainer.style.display = 'none'; // Hide the new chat container
    } else {
      existingChatContainer.style.display = 'block'; // Show the first chat container
    }
    if (message.awaiting === false) {
      userRectangle.classList.remove('awaiting-response');
    }


  } 
  chatBox = existingChatContainer.querySelector('.chat-box');
  // Update message logic remains the same
  // const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
  //   const adminResponseElement = el.querySelector('.admin-response');
  //   return adminResponseElement && adminResponseElement.textContent.includes('Awaiting Admin Response...');
  // });

  const awaitingMessageElement = Array.from(chatBox.getElementsByClassName('message')).find(el => {
    const adminResponseElement = el.querySelector('.admin-response');
    
    // régi: return adminResponseElement && adminResponseElement.textContent.includes('Awaiting Admin Response...');
    return adminResponseElement && 
    (adminResponseElement.textContent.includes('Awaiting Admin Response...') || 
     adminResponseElement.textContent.includes('Adminisztrátori válaszra várakozás...'));
  
    });


  const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
  if (awaitingMessageElement) {
    awaitingMessageElement.querySelector('.admin-response').textContent = message.admin_response
      ? `Admin: ${message.admin_response}`
      : awaitingText;


   
    chatBox.classList.add('not-awaiting-response');
    chatBox.classList.remove('awaiting-response');
    userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
    counterForManualModeAddMessage[tabIndex][message.user_id]+=1;

    
    if (!message.admin_response) {
      const adminResponseElement = awaitingMessageElement.querySelector('.admin-response');
        if (adminResponseElement) {
            const parentAdminMessage = adminResponseElement.closest('.admin-message');
            if (parentAdminMessage) {
                parentAdminMessage.remove();
            }
        }
     
      const messageElement = document.createElement('div');
      messageElement.className = 'message';

      const timestamp = message.timestamp || '';
      const awaitingText = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';

      // Create user message content and style it to align on the right
      //<span class="user-id">User ID: ${message.user_id}</span> taken out after timestamp
      const userMessageContent = `
        <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
          <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
        
          <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
        </div>
      `;
      // Create admin message content and style it to align on the left
      const adminMessageContent = `
        <div class="admin-message">
          ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : `<span class="admin-response">${awaitingText}</span>`}
        </div>
      `;

      // Append user message first (right-aligned) and admin message after (left-aligned)
      messageElement.innerHTML = userMessageContent + adminMessageContent;


      /////     ------    INSERTING THE MESSAGE     -----------
      
      const hiddenTimestampSpan = document.createElement('span');
      hiddenTimestampSpan.className = 'hidden-timestamp';
      hiddenTimestampSpan.style.display = 'none';
      hiddenTimestampSpan.textContent = timestamp;
      messageElement.appendChild(hiddenTimestampSpan);

        // Chronologically insert message based on normalized timestamps
      const newMessageTimestamp = new Date(timestamp);
      const existingMessages = Array.from(chatBox.getElementsByClassName('message'));

      let inserted = false;
      for (const existingMessage of existingMessages) {
        const hiddenTs = existingMessage.querySelector('.hidden-timestamp');
        if (hiddenTs) {
          const existingTs = new Date(hiddenTs.textContent.trim());
          if (newMessageTimestamp < existingTs) {
            chatBox.insertBefore(messageElement, existingMessage);
            inserted = true;
            break;
          }
        }
      }

      if (!inserted) {
        chatBox.appendChild(messageElement);
      }





      chatBox.classList.add('awaiting-response');
      chatBox.classList.remove('not-awaiting-response');
      userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
      counterForManualModeAddMessage[tabIndex][message.user_id]=0;
    } 
    
    // THE FIRST MESSAGE or SECONDLINE MESSAGE from THE USER HAS ARRIVED

  } else {
      if (message.user_message){   // brand new user message
        
      
        const messageElement = document.createElement('div');
        messageElement.className = 'message';

        const timestamp = message.timestamp || '';

        let headlineContent = "";
        let headlineText = language === 'hu' 
        ? "Átvett üzenet a következő kollégától: " 
        : "Messages from ";
          if (message.flag === "deleted" && message.message_number === 1) {
            headlineContent = `
                <div class="headline-message" style="background: linear-gradient(to right, rgba(144, 238, 144, 0.7), rgba(34, 139, 34, 0.7));">
                    ${headlineText} ${message.name}
                </div>
            `;

        }
       
        //<span class="user-id">User ID: ${message.user_id}</span> taken out after timestamp
        // Create user message content and style it to align on the right
        const userMessageContent = `
          <div class="user-message" style="background: linear-gradient(to right, rgb(151, 188, 252), rgb(183, 207, 250));">
            <span class="timestamp">${translation_Sent_User.sentAT}: ${formattedTime}</span>
           
            <span class="user-input">${translation_Sent_User.User}: ${message.user_message}</span>
          </div>
        `;
        // Create admin message content and style it to align on the left
        // const adminMessageContent = `
        //   <div class="admin-message">
        //     ${message.admin_response ? `<span class="admin-response">Admin: ${message.admin_response}</span>` : '<span class="admin-response">Awaiting Admin Response...</span>'}
        //   </div>
        // `;

        const adminMessageContent = `
          <div class="admin-message">
            ${
              message.bot_message
                ? message.bot_message === "Awaiting Admin Response..."
                  ? `<span class="admin-response">${message.bot_message}</span>` // If bot_message is "Awaiting Admin Response...", no Bot: prefix
                  : `<span class="admin-response">Bot: ${message.bot_message}</span>` // Otherwise, include Bot: prefix
                : message.admin_response
                  ? message.flag === "deleted"
                    ? `<span class="admin-response">${message.admin_response}</span>` // If message is deleted, no Admin: prefix
                    : `<span class="admin-response">Admin: ${message.admin_response}</span>` // Otherwise, include Admin: prefix
                  : '<span class="admin-response">Awaiting Admin Response...</span>' // If neither bot_message nor admin_response, show "Awaiting Admin Response..."
            }
          </div>
        `;

      

        // Append user message first (right-aligned) and admin message after (left-aligned)
        // messageElement.innerHTML = userMessageContent + adminMessageContent;
        messageElement.innerHTML =
        (headlineContent ? headlineContent : '') +
        userMessageContent +
        adminMessageContent;

        // Append hidden timestamp AFTER .innerHTML is set
        const hiddenTimestampSpan = document.createElement('span');
        hiddenTimestampSpan.className = 'hidden-timestamp';
        hiddenTimestampSpan.style.display = 'none';
        hiddenTimestampSpan.textContent = timestamp;
        messageElement.appendChild(hiddenTimestampSpan);


          
        // Insert based on timestamp ordering
        const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
        const insertBeforeIndex = chatMessages.findIndex(el => {
          const ts = el.querySelector('.hidden-timestamp');
          return ts && new Date(ts.textContent) > new Date(timestamp);
        });

        if (insertBeforeIndex === -1) {
          chatBox.appendChild(messageElement);
        } else {
          chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
        }



        chatBox.classList.add('awaiting-response');
        chatBox.classList.remove('not-awaiting-response');
        userRectangle.classList.add('awaiting-response'); // Add class to user rectangle
        counterForManualModeAddMessage[tabIndex][message.user_id]=0;
        // Check if bot_message exists and is not "Awaiting Admin Response..."
        if (message.bot_message && message.bot_message !== "Awaiting Admin Response...") {
          // Valid bot response: Remove awaiting-response classes
          chatBox.classList.remove('awaiting-response');
          userRectangle.classList.remove('awaiting-response');
          counterForManualModeAddMessage[tabIndex][message.user_id]=1;
        }
      
        if (message.awaiting === false) {
          chatBox.classList.remove('awaiting-response');
          userRectangle.classList.remove('awaiting-response');
          counterForManualModeAddMessage[tabIndex][message.user_id]=1;
        }
          

      }else {   //ITT A MÁSODIK ADMIN MESSAGET ADJUK HOZZÁ AMIKOR MÁR NINCS KIÍRVA, HOGY ADMIN VÁLSZRA VÁRVA...
        const messageElement = document.createElement('div');
        messageElement.className = 'message';

        const timestamp = message.timestamp || '';

      // chatBox.classList.add('not-awaiting-response');
      // chatBox.classList.remove('awaiting-response');
      // userRectangle.classList.remove('awaiting-response'); // Remove class from user rectangle
      // if (counterForManualModeAddMessage[tabIndex][message.user_id]>0){
        // Create admin message content and style it to align on the left
        const adminMessageContent = `
          <div class="admin-message">
            ${message.admin_response
              ? message.flag === "deleted"
                ? `<span class="admin-response">${message.admin_response}</span>`  // If message is deleted, no Admin: prefix
                : `<span class="admin-response">Admin: ${message.admin_response}</span>`  // Otherwise, include Admin: prefix
              : '<span class="admin-response"></span>'  // If no admin response, show "Awaiting Admin Response..."
            }
          </div>
        `;
        // Append user message first (right-aligned) and admin message after (left-aligned)
        messageElement.innerHTML = adminMessageContent;


        // Append real hidden timestamp element
        const hiddenTimestampSpan = document.createElement('span');
        hiddenTimestampSpan.className = 'hidden-timestamp';
        hiddenTimestampSpan.style.display = 'none';
        hiddenTimestampSpan.textContent = timestamp;
        messageElement.appendChild(hiddenTimestampSpan);

        // Insert into chatBox sorted by timestamp
        const chatMessages = Array.from(chatBox.getElementsByClassName('message'));
        const insertBeforeIndex = chatMessages.findIndex(el => {
          const tsEl = el.querySelector('.hidden-timestamp');
          return tsEl && new Date(tsEl.textContent) > new Date(timestamp);
        });

        if (insertBeforeIndex === -1) {
          chatBox.appendChild(messageElement);
        } else {
          chatBox.insertBefore(messageElement, chatMessages[insertBeforeIndex]);
        }



      // }
        if (message.awaiting === false) {
          counterForManualModeAddMessage[tabIndex][message.user_id]=1;
        }
        
    }
  }
  checkAwaitingResponse(tabIndex)

  // Get the bottom-right section for the current tab index
  const bottomRightSection = locations[tabIndex];

  // Check if a location box for the user already exists
  let userLocationBox = bottomRightSection.querySelector(`.location-box[data-user-id="${message.user_id}"]`);

  if (!userLocationBox) {
    // Create a new location box for the user if it does not exist
    userLocationBox = document.createElement('div');
    userLocationBox.className = 'location-box';
    userLocationBox.dataset.userId = message.user_id;
    userLocationBox.dataset.tabIndex = tabIndex;
    userLocationBox.style.overflowY = 'auto';
    userLocationBox.style.padding = '10px';
    userLocationBox.style.marginBottom = '5px';
    bottomRightSection.appendChild(userLocationBox);


    // Update the content of the location box with user data, including null values
    userLocationBox.innerHTML =  `
    <div style="margin-bottom: 10px;">
      <div class="user-id-header">
        <i class="fa-solid fa-user" style="margin-right: 5px;"></i>
        <span class="userID_text">${translation_location.userID}</span>
      </div>
      <div class="paddingleft" style="margin-top: 5px; font-size: clamp(7px, 0.8vw, 12px);">
        <span class="full-user-id-locbox">${message.user_id !== null ? message.user_id : 'No Data'}</span>
        <span class="truncated-user-id-locbox">${message.user_id !== null ? message.user_id.substring(0, 8) : 'No Data'}</span>
      </div>
    </div>

    <div style="margin-bottom: 10px;">
      <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(8px, 1.2vw, 15px); border-radius: 4px;">
        <i class="fa-solid fa-location-dot" style="margin-right: 5px;"></i>
        Location
      </div>
      <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(7px, 0.8vw, 12px);">
        
        ${message.location ?? 'No Data'}
      </div>
    </div>

    <div id="location-map" style="width: 100%; height: 200px; margin-top: 10px; border-radius: 4px;"></div>

    <div style="margin-bottom: 10px;">
      <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(5px, 1.2vw, 15px); border-radius: 4px;">
        <i class="fa-solid fa-arrows-left-right" style="margin-right: 5px;"></i>
        Longitude
      </div>
      <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
        
        ${message.longitude ?? 'No Data'}
      </div>
    </div>
    <div style="margin-bottom: 10px;">
      <div style="background-color: #ebeded; padding: 10px; font-weight: bold;  font-size: clamp(5px, 1.2vw, 15px); bold; border-radius: 4px;">
        <i class="fa-solid fa-arrows-up-down" style="margin-right: 5px;"></i>  
        Latitude
      </div>
      <div class="paddingleft" style="margin-top: 5px; display: flex; align-items: center; font-size: clamp(5px, 0.8vw, 12px);">
        
        ${message.latitude ?? 'No Data'}
      </div>
    </div>
    
  `;

  
  bottomRightSection.prepend(userLocationBox);


  // Initialize the map immediately after appending the content
  const mapContainer = userLocationBox.querySelector("#location-map");

  if (message.latitude !== null && message.longitude !== null) {
    const map = L.map(mapContainer).setView([message.latitude, message.longitude], 13);

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);

    L.marker([message.latitude, message.longitude]).addTo(map)
      .bindPopup("Location: " + message.location)
      .openPopup();
    // Ensure Leaflet resizes properly
    setTimeout(() => {
      map.invalidateSize();
      }, 300);

      requestAnimationFrame(() => {
          map.invalidateSize();
      });

      // Observe element visibility and refresh map when shown
      const visibilityObserver = new IntersectionObserver(entries => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                map.invalidateSize();
            }
        });
    }, { threshold: 0.1 });

    visibilityObserver.observe(userLocationBox);

    // Handle window resize events
    L.DomEvent.on(window, 'resize', () => map.invalidateSize());

  } else {
      // Hide the map container if no valid coordinates exist
      userLocationBox.querySelector("#location-map").style.display = "none";
  }

  if (bottomRightSection.querySelectorAll('.location-box').length > 1) {
    userLocationBox.style.display = 'none';
  }
 
  }


  userRectangle.addEventListener('click', () => {
    showLocationBox(message.user_id, tabIndex);
  });

  // // Show the latest chat container in the middle section
  // showUserChatBox(message.user_id, tabIndex);

  await new Promise(requestAnimationFrame);




}


    /////////////////////////////////                                 /////////////////////////////////
    //        SOCKET     ////////////                                 //      SOCKET       ////////////
    /////////////////////////////////                                 /////////////////////////////////
    
    




    /////////////////////////////////                                /////////////////////////////////
    //        SOCKET     ////////////                                //       SOCKET      ////////////
    /////////////////////////////////                                /////////////////////////////////


        // Now use these values in your socket connection
        const socket = io("https://redrain1230.loophole.site", {
            transports: ["websocket"],
            query: {
                user_id: user_id,
                user_org: user_org
            }
        });

        socket.on("history_start", (data) => {
          clearUIState();                  // Reset variables and DOM
          createDefaultTabForAutomaticMode();
          showInitialWaitingText()

          const overlay = document.getElementById('history-overlay');
          overlay.classList.add('show');
          startDotAnimation2();

          if (data.timezone) {
              clientTimezone = data.timezone;
              console.log("Client timezone set to:", clientTimezone);
          }
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                socket.emit("history_ready", {
                    socket_id: data.socket_id,
                    org: data.org,
                    user_id: user_id
                });
        });
      });
    });
  

    socket.on("force_logout", (data) => {
      alert(data.reason || "Session expired");
      window.location = "/logout";
    });

   
      





        
 


let messageDistribution = {};
let messageQueue = []; 
let manualModeReady = false;

// Function to process queued messages
function processMessageQueue2() {
  // Ensure both manualMode and messageDistribution are ready
  if (!manualMode || !manualModeReady) {
    return;
  }

  // Process the queued messages
  while (messageQueue.length > 0) {
    const queuedItem = messageQueue.shift();
    const { userId, message } = queuedItem;

    // Ensure the userId exists in messageDistribution
    const tabUniqueID = messageDistribution[userId];
    if (tabUniqueID) {
      appendMessageSavedStates(message, tabUniqueID);
    } else {
      console.error(`UserId ${userId} not found in messageDistribution`);
    }
  }
}


socket.on('new_message_FirstUser', function(payload) {

  (async () => {
 
  const messages = Array.isArray(payload.messages) ? payload.messages : [payload.messages];
  const totalMessagesToSend = payload.total_messages || messages.length
  
  
  messages.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

  
  sentMessageCount = 0;
  isBatchMode = totalMessagesToSend > 1;

  for (const message of messages) {
    console.log(" * * *  Sorted messages  * * *")
    console.log(message)
    await appendMessage(message);
  }

  if (!isBatchMode) {
    // If single message, reset flags immediately
    isBatchMode = false;
    totalMessagesToSend = 0;
    sentMessageCount = 0;
  }
    })();
 
});


//NEW


  //////////////////////// INTERNAL CHAT 
        window.addEventListener('beforeunload', () => {
            emitCloseTime();
        });

        
        const chatToggle = document.querySelector('.chat-icon');
        const chatDropdown = document.getElementById('chatDropdown');
        const chatMessages = document.getElementById('chatMessages');
        const chatForm = document.getElementById('chatForm');
        const chatInput = document.getElementById('chatInput');
        const chatAlert = document.getElementById('chatAlert');

        let dropdownIsOpen = false;

        function emitOpenTime() {
          socket.emit('admin_internal_message_open', {
            user_id: user_id,
            timestamp: Date.now()
          });
        }

        function emitCloseTime() {

          const timestamp = Date.now();
          // Save locally
          localStorage.setItem('adminInternalMessageClose', timestamp);


          socket.emit('admin_internal_message_close', {
            user_id: user_id,
            timestamp: timestamp
          });
        }

        chatToggle.addEventListener('click', (e) => {
          e.stopPropagation();

          if (chatDropdown.classList.contains('show')) {  //CLOSING
            chatDropdown.classList.remove('show');
            dropdownIsOpen = false;
            emitCloseTime();

          } else {  //OPENING ACT OF DROPDOWN
            chatDropdown.classList.add('show');
            chatInput.focus();
            chatAlert.style.display = 'none';
            dropdownIsOpen = true;
            emitOpenTime();

         
          }
        });

        document.addEventListener('click', (e) => {
          if (!chatDropdown.contains(e.target) && !chatToggle.contains(e.target)) {
            chatDropdown.classList.remove('show');
            dropdownIsOpen = false;
            emitCloseTime();
          }
        });

        // Store colors for users here
        const userColors = {};

        // Function to generate a random hex color
        // Function to generate strong, vivid (non-pastel) colors
        function getRandomColor() {
          let r, g, b, brightness;

          do {
            r = Math.floor(Math.random() * 256);
            g = Math.floor(Math.random() * 256);
            b = Math.floor(Math.random() * 256);
            brightness = 0.299 * r + 0.587 * g + 0.114 * b;
          } while (brightness > 160);  // Adjust this threshold as needed

          return `rgb(${r},${g},${b})`;
        }

        // Get or assign color for a user id
        function getColorForUser(userId) {
          if (!userColors[userId]) {
            userColors[userId] = getRandomColor();
          }
          return userColors[userId];
        }

        // Your existing event listener with small edits:
        chatForm.addEventListener('submit', (e) => {
          e.preventDefault();
          const msg = chatInput.value.trim();
          if (msg === "") return;

          const clientTimestamp = new Date().toISOString();

          console.log("Sending timestamp:", clientTimestamp);
          socket.emit('admin_internal_message', {
            user_id: user_id,
            message: msg,
            timestamp: clientTimestamp
          });

          const messageDiv = document.createElement('div');
          messageDiv.style.padding = '6px 8px';
          messageDiv.style.margin = '4px 0';
          messageDiv.style.backgroundColor = '#444';
          messageDiv.style.borderRadius = '4px';
          messageDiv.style.color = 'white';
          messageDiv.style.fontSize = '14px';
          messageDiv.style.wordBreak = 'break-word';
          messageDiv.style.whiteSpace = 'pre-wrap'; //
          messageDiv.style.width = '100%';
          messageDiv.style.boxSizing = 'border-box';
          messageDiv.dataset.timestamp = clientTimestamp;

          const language = localStorage.getItem('language') || 'hu';



          const ts = new Date(clientTimestamp);
          let tsString;

          // Hungarian style
          if(language === 'hu') {
              // Hungarian style: YYYY/DD/MM, 24h
              tsString = ts.getFullYear() + '/' +
                        String(ts.getDate()).padStart(2,'0') + '/' +
                        String(ts.getMonth()+1).padStart(2,'0') + ' ' +
                        String(ts.getHours()).padStart(2,'0') + ':' +
                        String(ts.getMinutes()).padStart(2,'0') + ':' +
                        String(ts.getSeconds()).padStart(2,'0');
          } else {
              // English style: MM/DD/YYYY, AM/PM
              tsString = ts.toLocaleString('en-US'); 
          }


          // Create timestamp div
          const tsDiv = document.createElement('div');
          tsDiv.style.fontSize = '11px';
          tsDiv.style.color = '#d7d6d6ff';
          tsDiv.style.marginBottom = '2px';
          tsDiv.textContent = tsString;

          messageDiv.appendChild(tsDiv);
          // Append message text after user id span
          messageDiv.appendChild(document.createTextNode(msg));

          // Insert messageDiv based on timestamp
          const existingMessages = Array.from(chatMessages.children);
          let inserted = false;

          for (let el of existingMessages) {
            const elTime = new Date(el.dataset.timestamp).getTime();
            const msgTime = new Date(clientTimestamp).getTime();
            if (msgTime < elTime) {
              chatMessages.insertBefore(messageDiv, el);
              inserted = true;
              break;
            }
          }

          // If no existing message is newer, append at the end
          if (!inserted) {
            chatMessages.appendChild(messageDiv);
          }



          //chatMessages.appendChild(messageDiv);
                    
          chatInput.value = '';
          chatInput.focus();
          
          // Scroll to bottom
          chatMessages.scrollTop = chatMessages.scrollHeight;
          
          // Hide alert if visible
          chatAlert.style.display = 'none';
          chatAlert.classList.remove('pulsing');
        });


socket.on('admin_internal_message', (data) => {
  const msg = data.message;
  const timestamp=data.timestamp;
  console.log("Timestamp----: ", timestamp)
  const senderId = data.sender_id;
  const name = data.name;
  if (senderId === user_id) return;

  // Outer wrapper for a whole message (vertical stack)
  const outerMessageWrapper = document.createElement('div');
  outerMessageWrapper.style.display = 'flex';
  outerMessageWrapper.style.flexDirection = 'column';
  outerMessageWrapper.style.borderRadius = '4px';
  outerMessageWrapper.style.backgroundColor = '#D1F2E5';
  outerMessageWrapper.style.margin = '4px 0';
  outerMessageWrapper.dataset.timestamp = timestamp;

  const language = localStorage.getItem('language') || 'hu';
  const ts = new Date(timestamp);
        let tsString;

        // Hungarian style
        if(language === 'hu') {
            // Hungarian style: YYYY/DD/MM, 24h
            tsString = ts.getFullYear() + '/' +
                      String(ts.getDate()).padStart(2,'0') + '/' +
                      String(ts.getMonth()+1).padStart(2,'0') + ' ' +
                      String(ts.getHours()).padStart(2,'0') + ':' +
                      String(ts.getMinutes()).padStart(2,'0') + ':' +
                      String(ts.getSeconds()).padStart(2,'0');
        } else {
            // English style: MM/DD/YYYY, AM/PM
            tsString = ts.toLocaleString('en-US'); 
        }

  // Create timestamp div
  const tsDiv = document.createElement('div');
  tsDiv.style.fontSize = '11px';
  tsDiv.style.color = '#757575ff';
  tsDiv.style.padding = '6px 8px 0 8px'; 
  tsDiv.textContent = tsString;

  const messageDiv = document.createElement('div');
  messageDiv.style.display = 'flex';
  messageDiv.style.flexDirection = 'row';
  messageDiv.style.alignItems = 'flex-start';
  messageDiv.style.padding = '2px 8px 6px 8px'; 
  messageDiv.style.margin = '4px 0';
  
  messageDiv.style.borderRadius = '4px';
  messageDiv.style.fontSize = '14px';
  messageDiv.style.color = 'black';
  

  // Left column: user name broken into lines
  const userNameDiv = document.createElement('div');
  userNameDiv.style.marginRight = '10px';
  userNameDiv.style.fontWeight = 'bold';
  userNameDiv.style.color = getColorForUser(name);
  userNameDiv.style.display = 'flex';
  userNameDiv.style.flexDirection = 'column';
  userNameDiv.style.justifyContent = 'flex-start';
  userNameDiv.style.minWidth = '80px';  // fix width for alignment

  // Split name by spaces, create a span per part, each on new line
  name.split(' ').forEach(part => {
    const partSpan = document.createElement('span');
    partSpan.textContent = part;
    userNameDiv.appendChild(partSpan);
  });

  // Right column: message text block
  const messageTextDiv = document.createElement('div');
  messageTextDiv.style.whiteSpace = 'pre-wrap';
  messageTextDiv.style.wordBreak = 'break-word';
  messageTextDiv.textContent = msg;

 

  

  
  messageDiv.appendChild(userNameDiv);
  messageDiv.appendChild(messageTextDiv);

  outerMessageWrapper.appendChild(tsDiv);
  outerMessageWrapper.appendChild(messageDiv);


  //chatMessages.appendChild(messageDiv);

  const existingMessages = Array.from(chatMessages.children);
  const msgTime = new Date(timestamp).getTime();
  let inserted = false;

  for (let el of existingMessages) {
    const elTime = new Date(el.dataset.timestamp).getTime();
    if (msgTime < elTime) {
      chatMessages.insertBefore(messageDiv, el);
      inserted = true;
      break;
    }
  }
  if (!inserted) {
    chatMessages.appendChild(outerMessageWrapper);
  }



  chatMessages.scrollTop = chatMessages.scrollHeight;

  const chatDropdown = document.getElementById('chatDropdown');
  if (!chatDropdown.classList.contains('show')) {
    chatAlert.style.display = 'inline';
    chatAlert.classList.add('pulsing');
  }
});




async function renderAdminInternalMessage(data) {
  const msg = data.message;
  const timestamp=data.timestamp;
  console.log("Raw timestamp:", timestamp);
  console.log("Parsed:", new Date(timestamp));

  const name=data.name
  const senderId = data.sender_id;

  const outerMessageWrapper = document.createElement('div');
  outerMessageWrapper.style.display = 'flex';
  outerMessageWrapper.style.flexDirection = 'column';
  outerMessageWrapper.style.borderRadius = '4px';
  outerMessageWrapper.style.backgroundColor = '#D1F2E5';
  outerMessageWrapper.style.margin = '4px 0';
  outerMessageWrapper.dataset.timestamp = timestamp;

  const language = localStorage.getItem('language') || 'hu';
  const ts = new Date(timestamp);
        let tsString;

        // Hungarian style
        if(language === 'hu') {
            // Hungarian style: YYYY/DD/MM, 24h
            tsString = ts.getFullYear() + '/' +
                      String(ts.getDate()).padStart(2,'0') + '/' +
                      String(ts.getMonth()+1).padStart(2,'0') + ' ' +
                      String(ts.getHours()).padStart(2,'0') + ':' +
                      String(ts.getMinutes()).padStart(2,'0') + ':' +
                      String(ts.getSeconds()).padStart(2,'0');
        } else {
            // English style: MM/DD/YYYY, AM/PM
            tsString = ts.toLocaleString('en-US'); 
        }

  // Create timestamp div
  const tsDiv = document.createElement('div');
  tsDiv.style.fontSize = '11px';
  tsDiv.style.color = '#757575ff';
  tsDiv.style.padding = '6px 8px 0 8px'; 
  tsDiv.textContent = tsString;



  const messageDiv = document.createElement('div');
  messageDiv.style.display = 'flex';
  messageDiv.style.flexDirection = 'row';
  messageDiv.style.alignItems = 'flex-start';
  messageDiv.style.padding = '2px 8px 6px 8px'; 
  messageDiv.style.margin = '4px 0';
  
  messageDiv.style.borderRadius = '4px';
  messageDiv.style.fontSize = '14px';
  messageDiv.style.color = 'black';

  if (senderId === user_id) {
    // Your own message: simple style, no username shown
    messageDiv.style.color = 'white';
    messageDiv.style.backgroundColor = '#444';
    messageDiv.textContent = msg;

    const existingMessages = Array.from(chatMessages.children);
    const msgTime = new Date(timestamp).getTime();
    let inserted = false;

    for (let el of existingMessages) {
      const elTime = new Date(el.dataset.timestamp).getTime();
      if (msgTime < elTime) {
        chatMessages.insertBefore(messageDiv, el);
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      chatMessages.appendChild(messageDiv);
    }
  } else {
    // Others' message: flex container with vertical name and message side by side
    messageDiv.style.display = 'flex';
    messageDiv.style.alignItems = 'flex-start';  // align to top for multiline text
    messageDiv.style.color = 'black';

    // Left column: name parts stacked vertically
    const userNameDiv = document.createElement('div');
    userNameDiv.style.marginRight = '10px';
    userNameDiv.style.fontWeight = 'bold';
    userNameDiv.style.color = getColorForUser(name);
    userNameDiv.style.display = 'flex';
    userNameDiv.style.flexDirection = 'column';
    userNameDiv.style.justifyContent = 'flex-start';
    userNameDiv.style.minWidth = '80px';

    name.split(' ').forEach(part => {
      const partSpan = document.createElement('span');
      partSpan.textContent = part;
      userNameDiv.appendChild(partSpan);
    });

    // Right column: message text block
    const messageTextDiv = document.createElement('div');
    messageTextDiv.style.whiteSpace = 'pre-wrap';
    messageTextDiv.style.wordBreak = 'break-word';
    messageTextDiv.textContent = msg;

  

  

    messageDiv.appendChild(userNameDiv);
    messageDiv.appendChild(messageTextDiv);

    outerMessageWrapper.appendChild(tsDiv);
    outerMessageWrapper.appendChild(messageDiv);

    const existingMessages = Array.from(chatMessages.children);
    const msgTime = new Date(timestamp).getTime();
    let inserted = false;

    for (let el of existingMessages) {
      const elTime = new Date(el.dataset.timestamp).getTime();
      if (msgTime < elTime) {
        chatMessages.insertBefore(messageDiv, el);
        inserted = true;
        break;
      }
    }
    if (!inserted) {
      chatMessages.appendChild(outerMessageWrapper);
    }
    
    ;


  }

  
  

  //chatMessages.appendChild(messageDiv);
  
  await new Promise(requestAnimationFrame)
  chatMessages.scrollTop = chatMessages.scrollHeight;


  //const chatDropdown = document.getElementById('chatDropdown');
  

  // if (!chatDropdown.classList.contains('show')) {
    
  //   chatAlert.style.display = 'inline';
  //   chatAlert.classList.add('pulsing');
  // }
}





function processMessageQueue() {
  messageQueue.sort((a, b) => new Date(a.message.timestamp) - new Date(b.message.timestamp));
  let i = 0;
  while (i < messageQueue.length) {
    const { message, tabUniqueID, specialArg } = messageQueue[i];

    if (tabUniqueID) {
      if (specialArg) {
        // Use the function for specialArg
        appendMessageSavedStatesForDragAndDropSocket(message, tabUniqueID, specialArg);
      } else {
        // Use the regular function
        appendMessageSavedStates(message, tabUniqueID);
      }
      messageQueue.splice(i, 1); // Remove the processed message
    } else {
      i++; // Skip unprocessable messages
    }
  }
}


    // When a new message is received
socket.on('new_message', function(message) {
  appendMessage(message);

});

// Listen for the 'message_distribution' event
socket.on('message_distribution', function(data) {
  console.log("@@@@@@@@@@@fffffffff")
  console.log(data)
  const message = data.message;
  const tabUniqueID = data.tab_uniqueId;
  const specialArg = data.specialArg; // Extract specialArg if present

  // Check if the data contains both message and tabUniqueId
  if (message && tabUniqueID && manualMode) {
    if (specialArg) {
      appendMessageSavedStatesForDragAndDropSocket(message, tabUniqueID, specialArg);
    } else {
      console.log("without special arg")
      appendMessageSavedStates(message, tabUniqueID);
    }
  } else {
    messageQueue.push({ message, tabUniqueID });
  }
});

socket.on('batch_ready_for_distribution', async function(batch) {
  const messages = Array.isArray(batch) ? batch : [batch];
  console.log("*** ELLENŐRZÉS *** ????????????????????????")
  console.log(messages)
  // Sort by message timestamp
  messages.sort((a, b) => {
    const timeA = new Date(a.message.timestamp).getTime();
    const timeB = new Date(b.message.timestamp).getTime();
    return timeA - timeB;
  });

  for (const entry of messages) {
    const message = entry.message;
    const tabUniqueID = entry.tab_uniqueId;
    const specialArg = entry.specialArg;

    if (specialArg) {
      console.log("specarg?")
      await appendMessageSavedStatesForDragAndDropSocket(message, tabUniqueID, specialArg);
    } else {
      console.log("no spec arg-----")
      await appendMessageSavedStates(message, tabUniqueID);
    }
  }
});


// IF WE HAVE MANUAL OR AUTOMATIC WORK OVERALL

// Handle mode change
socket.on('mode_changed', function (data) {
  const newMode = data.mode;
  manualMode = (newMode === 'manual');
  updateModeUI(newMode);

  if (manualMode) {
    if (messageQueue.length > 0) {
      processMessageQueue();
    } 
  } 
});


async function processEventsWithUIBreathing(events, perEventHandler) {
  for (let i = 0; i < events.length; i++) {
    await perEventHandler(events[i]);

    // Let the browser breathe
    await new Promise(resolve => setTimeout(resolve, 0));
  }
}




socket.on('event_log_batch', async function (payload) {   //async- tesszük ezt ami azt jelenti, hogy lehetővá válik async functionokat használni itt. Magától a function úgy lesz async hogy egy promisba wrappoljuk vagy adunk hozzá egy promist. a tabcreationnál csak a DOM nodok kreálását várjuk meg, de ez elég itt, az appendnél kell a kirjzolás is ezért adunk hozzá egy requestframe promist
  
  const overlay = document.getElementById('history-overlay');
  const chatAlert = document.getElementById('chatAlert');
  const events = payload.events || [];
  const showChatAlert = payload.show_chat_alert;

  events.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));


  const chatMessages = document.getElementById('chatMessages');
  chatMessages.innerHTML = '';

  console.log("Events length from payload:", events.length);
  console.log("Raw events array:", events);

  for (const event of events) {
    console.log("========================")
    console.log(event)
    console.log("========================")
    await handleSingleEvent(event);
    
  }

  if (chatAlert) {
  // Filter internal messages only
  const internalMessages = events.filter(e => e.event_type === 'admin_internal_message');

  // Find latest internal message timestamp
  const latestInternalTimestamp = internalMessages.length
    ? new Date(internalMessages[internalMessages.length - 1].timestamp).getTime()
    : 0;

  // Get locally stored close time (or 0 if not set)
  const localCloseTime = parseInt(localStorage.getItem('adminInternalMessageClose') || '0');

  // Show alert only if server says show AND there are messages after local close
  if (showChatAlert && latestInternalTimestamp > localCloseTime) {
    chatAlert.style.display = 'inline';
    chatAlert.classList.add('pulsing');
  } else {
    chatAlert.style.display = 'none';
    chatAlert.classList.remove('pulsing');
  }
}

  stopDotAnimation2();
  overlay.classList.remove('show');

});

async function handleSingleEvent(event) {
  // Acknowledge (if you still want to)
  // socket.emit('acknowledge_event', { status: 'received', event });

  if (event.event_type === 'tabs_created') {
    const tabs = event.data.tabs || [];
    await createTabsForClient(tabs);

  } else if (event.event_type === 'new_message') {
    const userId = event.data.user_id;
    const message = event.data;

    if (manualMode) {
      if (!manualModeReady || Object.keys(messageDistribution).length === 0) {
        messageQueue.push({ userId, message });
      } else {
        const tabUniqueID = messageDistribution[userId];
        if (tabUniqueID) {
          await appendMessageSavedStates(message, tabUniqueID);
        } else {
          console.error(`UserId ${userId} not found in messageDistribution`);
        }
      }
    } else {
      await appendMessage(message);
    }

  } else if (event.event_type === 'message_distribution') {
    const { message, tab_uniqueId, specialArg } = event.data;
    if (message && tab_uniqueId && manualMode) {
      if (specialArg) {
        await appendMessageSavedStatesForDragAndDropSocket(message, tab_uniqueId, specialArg);
      } else {
        await appendMessageSavedStates(message, tab_uniqueId);
      }
    } else {
      messageQueue.push({ message, tabUniqueID: tab_uniqueId, specialArg });
    }

  } else if (event.event_type === 'mode_changed') {
    manualMode = (event.data.mode === 'manual');
    await updateModeUI_promise(event.data.mode);
    if (manualMode && messageQueue.length > 0) {
      processMessageQueue();
    }

  } else if (event.event_type === 'response_state_changed') {
    automaticResponseStates[event.data.user_id] = event.data.state;
    counterForAddAdminMessage[event.data.user_id] = 0;
    updateAdminIntervention_fornewlyjoined(event.data.user_id, event.data.state);

  } else if (event.event_type === 'admin_response') {
    updateAdminIntervention_response(event.data);

  } else if (event.event_type === 'colleagues_input_updated') {
    input.value = String(event.data);

  } else if (event.event_type === 'one_colleague_added') {
    inputAddOneColleague.value = event.data;

  } else if (event.event_type === 'one_colleague_removed') {
    removeOneColleague.value = event.data;

  } else if (event.event_type === 'show_edit_tabs') {
    editTabCreation();

  } else if (event.event_type === 'show_tabs_input') {
    updateModeUIShowTabAgain("");

  } else if (event.event_type === 'colleague_added') {
    const tabs = event.data.tabs || [];
    addColleagueTab(tabs);

  } else if (event.event_type === 'colleague_removed') {
    removeColleagueforSocket_theRest(event.data);

  } else if (event.event_type === 'admin_response_manual_M') {
    updateAdminIntervention_response_ManualMode(event.data);

  } else if (event.event_type === 'tab_name_updated') {
    updateTabName(event.data.uniqueId, event.data.newName);

  } else if (event.event_type === 'admin_internal_message') {
          let eventData;
          try {
              eventData = typeof event.data === 'string' ? JSON.parse(event.data) : event.data;
          } catch (err) {
              console.error("Failed to parse event.data", err, event.data);
              eventData = {};
          }
          // Attach timestamp from outer event if needed
          eventData.timestamp = event.timestamp;
          await renderAdminInternalMessage(eventData);
  }
}

// socket.on('event_log_update', async function(event) {
  
//   socket.emit('acknowledge_event', {status: 'received', event: event});
//   // Check the event type and handle accordingly
//   if (event.event_type === 'tabs_created') {
//     const tabs = event.data.tabs || [];
//     await createTabsForClient(tabs);

//     // Send acknowledgment back to the backend
//     callback({ status: 'received' });

//   } else if (event.event_type === 'new_message') {
//     const userId = event.data.user_id;
//     const message = event.data;

    
//       if (manualMode){
//         if (!manualModeReady || Object.keys(messageDistribution).length === 0) {
//           // If messageDistribution is not populated, queue the message
//           messageQueue.push({userId: userId, message: message});
//         } else {
//           // If manualMode is ready, process the message immediately
//           const tabUniqueID = messageDistribution[userId];
//           if (tabUniqueID) {
//             appendMessageSavedStates(message, tabUniqueID);
//           } else {
//             console.error(`UserId ${userId} not found in messageDistribution`);
//           }
//         }
//       }else{
//         appendMessage(event.data); 
//       }
     
//   }else if (event.event_type === 'message_distribution'){
//     const message = event.data.message;
//     const tabUniqueID = event.data.tab_uniqueId;
//     const specialArg = event.data.specialArg; // Extract specialArg if present
 
//     // Check if the data contains both message and tabUniqueId
//     if (message && tabUniqueID && manualMode) {
//       if (specialArg) {
//         appendMessageSavedStatesForDragAndDropSocket(message, tabUniqueID, specialArg);
//       } else {
//         appendMessageSavedStates(message, tabUniqueID);
//       }
//     } else {
//       messageQueue.push({ message, tabUniqueID, specialArg });
//     }

   
//   } else if (event.event_type === 'mode_changed') {
//       manualMode = (event.data.mode === 'manual');
   
//       await updateModeUI_promise(event.data.mode);
//       if (manualMode) {
//         if (messageQueue.length > 0) {
//           processMessageQueue();
//         } 
//       } 

//       callback({ status: 'received' });
      
//   } else if (event.event_type === 'response_state_changed') {
    
//     automaticResponseStates[event.data.user_id]=event.data.state
//     counterForAddAdminMessage[event.data.user_id] = 0
//     updateAdminIntervention_fornewlyjoined(event.data.user_id, event.data.state);
//   } else if (event.event_type === 'admin_response') {
//     updateAdminIntervention_response(event.data);
//   } else if (event.event_type === 'colleagues_input_updated_') {
//     // Update the colleague input field based on the server-side data
//     input.value = String(event.data); // Assuming `input` is a reference to the colleague input field
//   } else if (event.event_type === 'one_colleague_added') {
//     // Add a new colleague to the UI and possibly create a new tab
//     inputAddOneColleague.value = event.data; // Assuming `inputAddOneColleague` is the input field
//   } else if (event.event_type === 'one_colleague_removed') {
//     // Update colleague removal data in the UI
//     removeOneColleague.value = event.data; // Assuming `removeOneColleague` is the removal input 
//   } else if (event.event_type === 'show_edit_tabs') {
//     // Trigger showing the edit tab interface
//     editTabCreation();
//   } else if (event.event_type === 'show_tabs_input') {
//     // Trigger showing the tab creation input fields
//     updateModeUIShowTabAgain(""); // You can customize this to show input or perform other actions
//   } else if (event.event_type === 'colleague_added') {
//     const tabData = event.data.tabs || [];
//     // Handle adding a colleague (e.g., adding a new tab)
//     addColleagueTab(tabData); // Assuming event.data is the colleague's name
//   } else if (event.event_type === 'colleague_removed') {
//     // Handle colleague removal (e.g., removing a tab)
//     removeColleagueforSocket_theRest(event.data); // Assuming event.data is the colleague's name
  
//   }  else if (event.event_type === 'admin_response_manual_M') {
//     updateAdminIntervention_response_ManualMode(event.data)
//   }else if (event.event_type === 'tab_name_updated') {
//     const updatedTab = event.data;
//     updateTabName(updatedTab.uniqueId, updatedTab.newName);
//   } else if (event.event_type === 'admin_internal_message') {
//     renderAdminInternalMessage(event.data);  // Historical
//   }


// });






// GENERALLY AUTOMATIC MODE: WHEN RECEVING THE STATE IF "INTERVENTION" OR "AUTOMATIC" BY CUSTOMERS VIA SOCKET AND USERS ARE LOGGED IN

// Listen for the server broadcast of updated states
socket.on('response_state_update', (data) => {
  // Update the automaticResponseStates with the data received from the server
  automaticResponseStates[data.user_id] = data.state;
  counterForAddAdminMessage[data.user_id] = 0
  
  // Update UI elements to reflect the new state
  updateAdminIntervention(data)
 
});

// WHEN RECEVING THE RESPONSE VIA SOCKET AND USERS ARE LOGGED IN
socket.on('response_update', function(data) {
  updateAdminIntervention_response(data);
 
});



// TABS CREATION INPUT VALUE

socket.on('colleagues_input_updated', (inputValue) => {
  input.value = String(inputValue); // Update input with received data
});

socket.on('one_colleague_addition', (inputValueAddOne) => {
  inputAddOneColleague.value = inputValueAddOne; // Update input with received data
});

socket.on('one_colleague_removal', (inputValueRemoveOne) => {
  removeOneColleague.value = inputValueRemoveOne;
});


// MANUAL MODE: Listen for tab creation broadcasts, WHEN PUSHING CREATE TABS BUTTON
socket.on('createTabs', (data) => {
  const tabs = data.tabs || []; // Get the array of tabs with name and uniqueId
  createTabsForClient(tabs);
});


// MANUAL MODE: RECEIVE MESSAGES WHEN WE ARE MANUAL MODE AND WE ARE NOT NEWLY LOGGER

socket.on('admin_response_broadcast_Manual', (data) => {
    updateAdminIntervention_response_ManualMode(data)
  });


// When clicking the EDIT BUTTON  
socket.on('show_edit_tabs', () => {
    if (currentTabMode !== 'edit') {
    currentTabMode = 'edit';
    editTabCreation(); // you can replace this with showEditTabs(), or updateModeUIEdit() for safety
  }
  });

  // When clicking the BACK BUTTON
socket.on('show_tabs_input', () => {
  if (currentTabMode !== 'input') {
    currentTabMode = 'input';
    updateModeUIShowTabAgain(""); // or showTabsInput()
  }
  });


// Listen for the 'clear_input_field' event from the server
socket.on('clear_input_field', () => {
  const inputField = document.getElementById('colleagues');
  if (inputField) {
    inputField.value = '';  // Clear the input field for the current user
    
  }
});



socket.on('colleague_added', (data) => {
  const tabData = data.tabs || [];
  // Call the tab creation function with the received colleague name
  addColleagueTab(tabData);
  });
  

socket.on('colleague_removed', (removedColleagueName) => {
    // Call the tab creation function with the received colleague name
    removeColleagueforSocket(removedColleagueName);
    });

socket.on('tab_name_updated', (data) => {
  const { uniqueId, newName } = data;
  // Call the function to update the tab with the new name
  updateTabName(uniqueId, newName);
    });



//-----------------------------  LANGUAGE --------------------


document.getElementById('lang-en').addEventListener('click', function() {
  console.log("Before Language Change (EN) - userLocationBox count: ", document.querySelectorAll('.userLocationBox').length);

  
  localStorage.setItem('language', 'en');
  setLanguage('en');

  console.log("After Language Change (EN) - userLocationBox count: ", document.querySelectorAll('.userLocationBox').length);
});

document.getElementById('lang-hu').addEventListener('click', function() {
  
  console.log("Before Language Change (HU) - userLocationBox count: ", document.querySelectorAll('.userLocationBox').length);
  localStorage.setItem('language', 'hu');
  setLanguage('hu');
  console.log("After Language Change (HU) - userLocationBox count: ", document.querySelectorAll('.userLocationBox').length);
});

// Set language on page load
window.onload = function() {
  if (typeof translations !== 'undefined') {
  const language = localStorage.getItem('language') || 'hu';
  setLanguage(language);
  }
};


function setLanguage(language) {
  document.querySelectorAll('[data-lang]').forEach(element => {
      const langKey = element.getAttribute('data-lang');
      if (translations[language][langKey]) {
          element.innerHTML = translations[language][langKey];
      }
  });

   // Handle placeholders for inputs/textareas
  document.querySelectorAll('[data-lang-placeholder]').forEach(element => {
    const langKey = element.getAttribute('data-lang-placeholder');
    if (translations[language][langKey]) {
      element.placeholder = translations[language][langKey];
    }
  });

  // Update dynamically changing elements
  updateDynamicText(language);
}


const translations = {
  en: {
    toggleMode: "Switch to Manual Response",
    home: "Home",
    dashboard: "Dashboard",
    metrics:"ChatBot Metrics & Insights",
    prediction:"Advanced AI Predictive Solutions",
    toggleModeManual: "Switch to Automatic Response",
    adminDash: "Admin Dashboard",
    automaticChatbot: 'Automatic Chatbot',
    customers:"Customers",
    customerDetails:"Customer details",
    waiting:"Waiting for user queries",
    colleaguesLabel: "Colleagues:",
    colleaguesPlaceholder:"E.g., Jennifer, Frank",
    addColleagueLabel: "Add a Colleague",
    removeColleagueLabel: "Remove a Colleague",
    createTabsButton: "Create Tabs",
    editTabsButton: "Edit Tabs",
    backButton: "Back",
    addColleagueButton: "Add",
    additionalColleaguePlaceholder:"A colleague's name",
    removeAdditionalColleaguePlaceholder:"A colleague's name",
    removeColleagueButton: "Remove",
    automaticResponse:"Admin Intervention",
    manualIntervention:"Automatic Response",
    chats: "Chats",
    logout: "Logout",
    chatTitle: "Administrator Chat",
    chatPlaceholder: "Message to colleagues...",
    chatSendButton: "Send",
    historyLoading: "Reloading chat history",
    
    
  },
  hu: {
    chatTitle: "Adminisztrátori Chat",
    chatPlaceholder: "Üzenet a kollégáknak...",
    chatSendButton: "Küld",
    toggleMode: "Manuális válaszadás",
    home: "Kezdőlap",
    dashboard: "Kezelőpult",
    metrics:"Chatbot mutatók és elemzések", 
    prediction:"Prediktív AI-megoldások",
    toggleModeManual: "Automatikus válaszadás",
    adminDash: "Admin irányítópult",
    automaticChatbot:"Automatikus Chatbot",
    customers:"Ügyfelek",
    customerDetails:"Ügyféladatok",
    waiting:"Várakozás az ügyfelek kérdéseire",
    colleaguesLabel: "Üzenetkezelők:",
    colleaguesPlaceholder:"pl.: Annabella, András",
    addColleagueLabel: "Felvétel:",
    removeColleagueLabel: "Törlés:",
    createTabsButton: "Létrehozás",
    editTabsButton: "Szerkesztés",
    backButton: "Vissza",
    addColleagueButton: "Hozzáadás",
    additionalColleaguePlaceholder:"Egy kolléga neve",
    removeAdditionalColleaguePlaceholder:"Egy kolléga neve",
    removeColleagueButton: "Eltávolítás",
    automaticResponse:"Admin közbelépés",
    manualIntervention:"Automatikus válaszadás",
    chats: "Üzenetek",
    logout: "Kijelentkezés",
    historyLoading: "Előzmények betöltése",

  }
};

function getTranslations(language) {
  return {
    userID: language === 'hu' ? 'Ügyfélazonosító' : 'User-ID',
    location: language === 'hu' ? 'Hely' : 'Location',
    longitude: language === 'hu' ? 'Hosszúság' : 'Longitude',
    latitude: language === 'hu' ? 'Szélesség' : 'Latitude',
    sentAT: language === 'hu' ? 'A Küldés ideje' : 'Sent at',
    User: language === 'hu' ? 'Ügyfél' : 'User',
  };
}

function updateDynamicText(language) {

  const toggleButton = document.getElementById('toggle-response-mode');
  const manualMode = toggleButton.textContent.includes(translations['en']['toggleModeManual']); // Check current mode


  toggleButton.textContent = manualMode ? translations[language]['toggleModeManual'] : translations[language]['toggleMode'];
  const topLeftSection = document.querySelector('.top-left-section');
  if (topLeftSection) {
    topLeftSection.textContent = translations[language]['customers'];
  }


  const topMiddleSection = document.querySelector('.top-middle-section');
  if (topMiddleSection) {
      const chatsLabel = topMiddleSection.querySelector('.button-container span');
      if (chatsLabel) {
          chatsLabel.textContent = translations[language]['chats'];
      }

      //topMiddleSection.textContent = language === 'hu' ? 'Üzenetek' : 'Chats';
  }


  const automaticChatbotTab = document.querySelector('#automatic-chatbot-tab'); // Assuming you give the tab an ID or class
  if (automaticChatbotTab) {
    automaticChatbotTab.textContent = translations[language]['automaticChatbot'] || 'Automatic Chatbot'; // Fallback to English if translation not found
  }

  document.querySelectorAll('.admin-intervention').forEach(button => {
    const userId = button.getAttribute('data-user-id');

    // Ensure state exists for this user
    if (automaticResponseStates[userId] === undefined) {
      automaticResponseStates[userId] = false;  // Default: Automatic Mode
  }
    const isAutomatic = automaticResponseStates[userId];

    
    // Update text based on mode
    button.textContent = isAutomatic 
        ? translations[language]['manualIntervention'] //Automatikus válaszadás
        : translations[language]['automaticResponse']; //Admin válaszadás
    
    console.log("Text 1 ", button.textContent)
    
    
   
  });

    // UPDATING THE SEND RESPONSE BUTTON AND THE PLACEHOLDER
    // Update all instances of manual response placeholders
    document.querySelectorAll('.manual-response').forEach(textarea => {
      textarea.placeholder = language === 'hu' ? 'Írd ide az üzeneted...' : 'Type your response...';
    });
  
    // Update all instances of send response buttons
    document.querySelectorAll('.send-response').forEach(button => {
      button.textContent = language === 'hu' ? 'Küldés' : 'Send Response';
    });


    //AUTOMATIC MODE: AWAITING ADMIN RESPONSE HANDLING
    // Update "Awaiting Admin Response..." translation
    document.querySelectorAll('.admin-response').forEach(span => {
      if (span.textContent.trim() === 'Awaiting Admin Response...' || span.textContent.trim() === 'Adminisztrátori válaszra várakozás...') {
        span.textContent = language === 'hu' ? 'Adminisztrátori válaszra várakozás...' : 'Awaiting Admin Response...';
      }
    });

    //LOCATIONBOX

    const translations2_loc = getTranslations(localStorage.getItem('language') || 'hu');
    


    document.querySelectorAll('.location-box').forEach(userLocationBox => {
      if (!userLocationBox) return;
     
      // Target the elements 
      const userIdElement = userLocationBox.querySelector('.userID_text');
      if (userIdElement) {
          userIdElement.textContent = translations2_loc.userID;  // Update the text
      }

      const locationElement = userLocationBox.querySelector('.location_userlocationbox');
      if (locationElement) {
        locationElement.textContent = translations2_loc.location;  // Update the text
      }

      const longitudeElement = userLocationBox.querySelector('.longitude_userlocationbox');
      if (longitudeElement) {
        longitudeElement.textContent = translations2_loc.longitude;  // Update the text
      }

      const latitudeElement = userLocationBox.querySelector('.latitude_userlocationbox');
      if (latitudeElement) {
        latitudeElement.textContent = translations2_loc.latitude;  // Update the text
      }
  });

  //MANUAL MODE SENT AT , USER
  document.querySelectorAll('.user-message').forEach(userMessage => {
    const timestampElement = userMessage.querySelector('.timestamp');
    const userInputElement = userMessage.querySelector('.user-input');

    if (timestampElement) {
      const timestampText = timestampElement.textContent.split(': ')[1] || ''; // Preserve timestamp value
      timestampElement.textContent = `${translations2_loc.sentAT}: ${timestampText}`;
    }

    if (userInputElement) {
      const userMessageText = userInputElement.textContent.split(': ')[1] || ''; // Preserve user message
      userInputElement.textContent = `${translations2_loc.User}: ${userMessageText}`;
    }
  });

  // MESSAGE FROM:

  let headlineMessages = document.querySelectorAll(".headline-message");
    
        headlineMessages.forEach(headlineMessage => {
          let currentText = headlineMessage.textContent.trim();
          console.log("Current headline text:", currentText); // Debugging
          
          let messageName = ""; 

          // Extract the colleague's name based on the existing language format
          if (currentText.startsWith("Átvett üzenet a következő kollégától: ")) {
              messageName = currentText.replace("Átvett üzenet a következő kollégától: ", "");
          } 
          else if (currentText.startsWith("Messages from ")) {
              messageName = currentText.replace("Messages from ", "");
          }

          console.log("Extracted name:", messageName);
          // Define the new translated text
          let newHeadlineText = language === 'hu' 
              ? "Átvett üzenet a következő kollégától: " 
              : "Messages from ";

          // Update each headline dynamically
          headlineMessage.textContent = `${newHeadlineText} ${messageName}`;
      });
    
    
    
    



  const topRightSection = document.querySelector('.top-right-section');
  if (topRightSection) {
    topRightSection.textContent = translations[language]['customerDetails'] || 'Customer details'; // Fallback to English if translation is not found
  }

   // Update the "Waiting for user queries" animation
   const waitingTextElement = document.querySelector('.initial-text'); // Select the waiting text element
   if (waitingTextElement) {
       clearInterval(dotAnimation); // Stop current animation
       waitingTextElement.dataset.baseText = translations[language]['waiting'] || 'Waiting for user queries'; // Update base text
       waitingTextElement.textContent = waitingTextElement.dataset.baseText; // Reset text without dots
       startDotAnimation(waitingTextElement); // Restart animation with new text
   }

   // Update input placeholder dynamically
   const colleaguesInput = document.getElementById('colleagues');
   if (colleaguesInput) {
       colleaguesInput.placeholder = translations[language]['colleaguesPlaceholder'] || 'E.g., Jennifer, Frank';
   }

   const additionalColleague = document.getElementById('add-colleague');
   if (additionalColleague) {
    additionalColleague.placeholder = translations[language]['additionalColleaguePlaceholder'] || 'E.g., Jennifer, Frank';
   }

   const removeAdditionalColleague = document.getElementById('remove-colleague');
   if (removeAdditionalColleague) {
    removeAdditionalColleague.placeholder = translations[language]['removeAdditionalColleaguePlaceholder'] || 'E.g., Jennifer, Frank';
   }
   
  document.querySelector('label[for="colleagues"]').textContent = translations[language]['colleaguesLabel'];
  document.querySelector('label[for="add-colleague"]').textContent = translations[language]['addColleagueLabel'];
  document.querySelector('label[for="remove-colleague"]').textContent = translations[language]['removeColleagueLabel'];
  document.getElementById('create-tabs-button').textContent = translations[language]['createTabsButton'];

  const addColleagueButton = document.getElementById('add-colleague-button');
  if (addColleagueButton) {
      addColleagueButton.textContent = translations[language]['addColleagueButton'];
  }

  const removeColleagueButton = document.getElementById('remove-colleague-button');
  if (removeColleagueButton) {
      removeColleagueButton.textContent = translations[language]['removeColleagueButton'];
  }

  const editTabsButton = document.getElementById('edit-tabs-button');
  if (editTabsButton) {
      editTabsButton.textContent = translations[language]['editTabsButton'];
  }

  const backButton = document.getElementById('back-button');
  if (backButton) {
      backButton.textContent = translations[language]['backButton'];
  }
}


function clearUIState() {
  // Clear DOM containers

  if (dotAnimation !== null) {
    clearInterval(dotAnimation);
    dotAnimation = null;
  }

  const oldText = document.querySelector('.initial-text');
  if (oldText) oldText.remove();
  
  tabsContainer.innerHTML = '';
  tabContentsContainer.innerHTML = '';

  // Clear object contents instead of reassigning
  for (const key in colleaguesChats) delete colleaguesChats[key];
  for (const key in rectangle) delete rectangle[key];
  for (const key in locations) delete locations[key];
  for (const key in activeRectangles) delete activeRectangles[key];
  for (const key in isUserRectangleClickedPerTab) delete isUserRectangleClickedPerTab[key];
  for (const key in topMiddleButtons) delete topMiddleButtons[key];
  for (const key in Chats_automatic) delete Chats_automatic[key];
  for (const key in rectangle_automatic) delete rectangle_automatic[key];
  for (const key in locations_automatic) delete locations_automatic[key];
  for (const key in userElements) delete userElements[key];
  for (const key in userButtons) delete userButtons[key];
  for (const key in userButtonStates) delete userButtonStates[key];
  for (const key in automaticResponseStates) delete automaticResponseStates[key];
  for (const key in counterForAddAdminMessage) delete counterForAddAdminMessage[key];
  for (const key in counterForManualModeAddMessage) delete counterForManualModeAddMessage[key];

  // Reset primitives / booleans
  manualMode = false;
  isTabCreated = false;
  activeRectangle = null;
  isUserRectangleClicked = false;
  messageCount = 0;
  prependeduserId = 0;

  const tabsInputContainer = document.getElementById('tabs-input-container');
  const editTabsContainer = document.getElementById('edit-tabs-container');
  tabsInputContainer.style.display = 'none';
  editTabsContainer.style.display = 'none';

  currentTabMode = 'input';  // reset to default

  console.log("[UI Reset] UI state cleared and variables reset.");
}




function startDotAnimation2() {
  const dotEl = document.getElementById("dot-animation");
  let dotCount = 1;
  dotInterval_historyloading = setInterval(() => {
    dotCount = (dotCount % 3) + 1;
    dotEl.textContent = ".".repeat(dotCount);
  }, 500);
}

function stopDotAnimation2() {
  clearInterval(dotInterval_historyloading);
  document.getElementById("dot-animation").textContent = ".";
}



























// BUILDING DOM NODE IN MEMORY ///////////////////////
