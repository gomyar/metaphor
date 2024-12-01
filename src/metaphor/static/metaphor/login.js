
class Login {
    constructor(metaphor) {
        this.metaphor = metaphor;
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
                this.metaphor.load_schema();                
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


