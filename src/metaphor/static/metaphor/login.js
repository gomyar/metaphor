
class Login {
    constructor() {
        this.username = null;
        this.password = null;
        this.is_shown = false;
        this.error = null;
    }

    show_login() {
        this.username = null;
        this.password = null;
        this.is_shown = true;
        this.error = null;
        turtlegui.reload();
    }

    attempt_login() {
        turtlegui.ajax.post(
            '/login',
            {"username": this.username, "password": this.password},
            (response) => {
                this.cancel_login();
                load_initial_api();                
            },
            (err) => {
                if (err.status == 401) {
                    this.error = JSON.parse(err.responseText).error;
                } else {
                    this.error = err.responseText;
                }
                turtlegui.reload();
            });
    }

    cancel_login() {
        this.username = null;
        this.password = null;
        this.is_shown = false;
        this.error = null;
        turtlegui.reload();
    }
}


