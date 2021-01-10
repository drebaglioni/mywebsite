import React, {Component} from "react"
import "./pages.css"
import dre from "./dre.png"
import {NavLink} from "react-router-dom"


class Homepage extends Component {
    render() {
        return (
            <>


                <div className="col-12 wrapper" id="topWrapper">
                    <div className="row">


                        <div className="col-10 mainDivs" id="leftDiv">

                            <h1 className="mainTitle">andrea baglioni</h1>

                        </div>
                        <div className="col-2 mainDivs" id="rightDiv">

                            <img src={dre}></img>

                        </div>

                    </div>
                </div>


                <div className="col-12 wrapper" id="topWrapper">
                    <div className="row">


                        <div className="col-7 mainDivs" id="thirdDiv">

                            <img src={dre}></img>

                        </div>
                        <div className="col-5 mainDivs" id="fourthDiv">

                            <NavLink className="navHome" to="/resume">
                                Resume
                            </NavLink>
                            <br></br>
                            <NavLink className="navHome" to="/contact">
                                Contact Me
                            </NavLink>
                            <br></br>
                            <NavLink className="navHome" to="/about">
                                About
                            </NavLink>

                        </div>

                    </div>
                </div>

            </>
        )


    }


}
export default Homepage
