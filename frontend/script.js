// API base URL
const API_BASE_URL = 'http://localhost:8001';

// Format currency
function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(amount);
}

// Create business card HTML
function createBusinessCard(business) {
    return `
        <div class="business-card">
            <h3>${business.BorrowerName || 'Unknown Business'}</h3>
            <p><i class="fas fa-map-marker-alt"></i> ${business.BorrowerAddress || 'No address'}</p>
            <p><i class="fas fa-city"></i> ${business.BorrowerCity || ''}, ${business.BorrowerState || ''}</p>
            <p><i class="fas fa-dollar-sign"></i> Initial Amount: <span class="amount">${formatCurrency(business.InitialApprovalAmount || 0)}</span></p>
            <p><i class="fas fa-hand-holding-usd"></i> Forgiveness: <span class="amount">${formatCurrency(business.ForgivenessAmount || 0)}</span></p>
        </div>
    `;
}

// Show loading state
function showLoading(elementId, message) {
    document.getElementById(elementId).innerHTML = `
        <div class="loading">
            <i class="fas fa-circle-notch fa-spin"></i>
            ${message}
        </div>
    `;
}

// Show error state
function showError(elementId, message) {
    document.getElementById(elementId).innerHTML = `
        <div class="error">
            <i class="fas fa-exclamation-circle"></i>
            ${message}
        </div>
    `;
}

// Search businesses
async function searchBusinesses() {
    const name = document.getElementById('businessName').value.trim();
    const state = document.getElementById('state').value.trim().toUpperCase();
    const city = document.getElementById('city').value.trim();

    if (!name) {
        showError('searchResults', 'Please enter a business name');
        return;
    }

    showLoading('searchResults', 'Searching businesses...');

    try {
        let url = `${API_BASE_URL}/loans/search/by-borrower?borrower_name=${encodeURIComponent(name)}`;
        if (state) url += `&borrower_state=${encodeURIComponent(state)}`;
        if (city) url += `&borrower_city=${encodeURIComponent(city)}`;

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }

        const data = await response.json();

        const resultsDiv = document.getElementById('searchResults');
        if (data.length === 0) {
            showError('searchResults', 'No results found for your search');
            return;
        }

        resultsDiv.innerHTML = data.map(business => createBusinessCard(business)).join('');
    } catch (error) {
        console.error('Error:', error);
        showError('searchResults', 'Error fetching results. Please try again.');
    }
}

// Fetch top borrowers
async function fetchTopBorrowers() {
    showLoading('topBorrowers', 'Loading top borrowers...');

    try {
        const response = await fetch(`${API_BASE_URL}/loans/top-borrowers`);
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }

        const data = await response.json();
        const topBorrowersDiv = document.getElementById('topBorrowers');
        
        if (data.length === 0) {
            showError('topBorrowers', 'No top borrowers data available');
            return;
        }

        topBorrowersDiv.innerHTML = data.map(business => createBusinessCard(business)).join('');
    } catch (error) {
        console.error('Error:', error);
        showError('topBorrowers', 'Error loading top borrowers. Please refresh the page.');
    }
}

// Search by loan number
async function searchByLoanNumber() {
    const loanNumber = document.getElementById('loanNumber').value.trim();
    if (!loanNumber) {
        showError('loanSearchResult', 'Please enter a loan number');
        return;
    }

    showLoading('loanSearchResult', 'Searching loan...');

    try {
        const response = await fetch(`${API_BASE_URL}/loans/search/by-loan-number?loan_number=${encodeURIComponent(loanNumber)}`);
        if (!response.ok) {
            if (response.status === 404) {
                showError('loanSearchResult', 'Loan not found');
            } else {
                throw new Error('Network response was not ok');
            }
            return;
        }

        const business = await response.json();
        document.getElementById('loanSearchResult').innerHTML = createBusinessCard(business);
    } catch (error) {
        console.error('Error:', error);
        showError('loanSearchResult', 'Error fetching loan. Please try again.');
    }
}

// Add input validation
document.getElementById('state').addEventListener('input', function(e) {
    this.value = this.value.toUpperCase();
    if (this.value.length > 2) {
        this.value = this.value.slice(0, 2);
    }
});

// Add enter key support for search
document.addEventListener('keypress', function(e) {
    if (e.key === 'Enter') {
        const activeElement = document.activeElement;
        if (activeElement.id === 'businessName' || 
            activeElement.id === 'state' || 
            activeElement.id === 'city') {
            searchBusinesses();
        }
    }
});

// Load top borrowers when page loads
document.addEventListener('DOMContentLoaded', fetchTopBorrowers); 