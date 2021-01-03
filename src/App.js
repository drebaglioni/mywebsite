import logo from './logo.svg';
import './App.css';
import Homepage from "./pages/home"
import About from "./pages/about"
import Contact from "./pages/contact"
import {Switch, BrowserRouter, Route} from 'react-router-dom';

function App() {
    return (
        <div className="App">
            <BrowserRouter>
                <Switch>
                    <Route exact path="/"
                        component={Homepage}/>

                    <Route exact path="/about"
                        component={About}/>

                    <Route exact path="/contact"
                        component={Contact}/>

                </Switch>
            </BrowserRouter>
        </div>
    );
}

export default App;
