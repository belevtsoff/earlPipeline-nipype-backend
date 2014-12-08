earlPipeline-nipype-backend
===========================

Some nipype interfaces wrapped around as a backend for the earlPipeline GUI.

This is a very early and experimantal version of the wrapper. We'll first focus on implementing some of the DTI-related nipype interfaces to FSL. Check `nipype_wrapper_interfaces.py` to see which interfaces are already wrapped.

To run the server, you'll need a fresh clone of [earlPipeline](https://github.com/belevtsoff/earlPipeline) and, of course, [nipype](https://github.com/nipy/nipype). Then, create an empty folder named `pipelines`, fire `python2 server.py` and go to [http://localhost:54123](http://localhost:54123)
