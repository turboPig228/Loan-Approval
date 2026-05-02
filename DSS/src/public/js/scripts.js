$(function () {
    // Logout functionality (works on all pages)
    $("#logoutBtn").on("click", () => {
        if (confirm("Are you sure you want to log out?")) {
            localStorage.removeItem("token");
            window.location.href = "login.html";
        }
    });

    // Author Info button click event
    $(".author-btn").on("click", function () {
        window.location.href = "author.html";
    });

    // Access Control for predictor.html
    if (window.location.pathname.includes("predictor.html")) {
        const token = localStorage.getItem("token");
        console.log("Current token:", token);

        if (!token) {
            window.location.href = "login.html";
            return;
        }

        try {
            const decoded = jwt_decode(token);
            const currentTime = Math.floor(Date.now() / 1000);
            if (decoded.exp && decoded.exp < currentTime) {
                alert("Session expired! Please log in again.");
                localStorage.removeItem("token");
                window.location.href = "login.html";
                return;
            }
            console.log("Token decoded successfully for user:", decoded.username);
        } catch (error) {
            console.error("Token decoding failed:", error);
            alert("Invalid token! Please log in again.");
            localStorage.removeItem("token");
            window.location.href = "login.html";
            return;
        }

        // Fetch prediction logs
        $("#viewLogsBtn").on("click", function () {
            // ... логика логов без изменений ...
        });

        // Auto-fetch logs on page load with #logs hash
        if (window.location.hash === "#logs") {
            $("#viewLogsBtn").click();
        }
    }

    // Login button click event (for login.html)
    $(document).on("click", "#loginBtn", () => {
        const username = $("#username").val();
        const password = $("#password").val();
        $.ajax({
            url: "/login",
            method: "POST",
            contentType: "application/json",
            data: JSON.stringify({ username, password }),
            success: (response) => {
                localStorage.setItem("token", response.token);
                const decoded = jwt_decode(response.token);
                console.log("Login successful, user role:", decoded.role);

                switch (decoded.role) {
                    case 1:
                        window.location.href = "dashboard.html";
                        break;
                    case 2:
                        window.location.href = "registered-user.html";
                        break;
                    case 3:
                        window.location.href = "moderator.html";
                        break;
                    default:
                        window.location.href = "unauthorized.html";
                }
            },

            error: () => {
                $("#errorMsg").text("Invalid credentials").removeClass("hidden");
            }
        });
    });
});
