Nucleus Download Reports
=========================

This is the component to handle the Download of Reports for Archived Classes

This project contains multiple verticles which are deployed by one verticle. These verticles cater to different
functions like validating/authorizing the request, user provisioning, class creation, association of class and 
users etc.

To incorporate a new Verticle, it needs to be registered in VerticleRegistry which will then be used to deploy 
the verticle.

